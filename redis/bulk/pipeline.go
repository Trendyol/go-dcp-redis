package bulk

import (
	"os"
	"sync/atomic"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/Trendyol/go-dcp-redis/redis"
	"github.com/Trendyol/go-dcp-redis/redis/client"
	"github.com/Trendyol/go-dcp/logger"
	goredis "github.com/redis/go-redis/v9"
)

// retryableErrorPrefixes are Redis error codes that describe a temporary condition of the
// server (memory back-pressure, loading, failover, cluster resharding), not a bad command.
var retryableErrorPrefixes = []string{
	"OOM", "BUSY", "LOADING", "TRYAGAIN", "CLUSTERDOWN", "MASTERDOWN", "READONLY",
}

// flushBatch writes one batch: one pipeline per Redis client, clients in parallel. Commands
// keep their order within a client, so writes to the same key are applied in DCP order.
func (b *Bulk) flushBatch(batch []item) error {
	if len(batch) == 0 {
		return nil
	}
	startedTime := time.Now()

	groups := groupByClient(batch)
	var err error
	if len(groups) == 1 {
		err = b.execute(groups[0].client, groups[0].cmds)
	} else {
		err = b.executeGroups(groups)
	}

	dbgN.Add(1); dbgCmds.Add(int64(len(batch))); dbgNs.Add(int64(time.Since(startedTime)))
	if dbgN.Load()%200 == 0 { fmt.Fprintf(os.Stderr, "DBG flushes=%d avgBatch=%.1f avgFlushUs=%.0f\n", dbgN.Load(), float64(dbgCmds.Load())/float64(dbgN.Load()), float64(dbgNs.Load())/float64(dbgN.Load())/1000) }
	b.bulkRequestProcessLatencyMs.Store(time.Since(startedTime).Milliseconds())
	b.bulkRequestSize.Store(int64(len(batch)))
	return err
}

type clientGroup struct {
	client client.RedisClient
	cmds   []*redis.Command
}

func groupByClient(batch []item) []clientGroup {
	first := batch[0].client
	single := true
	for i := 1; i < len(batch); i++ {
		if batch[i].client != first {
			single = false
			break
		}
	}
	if single {
		cmds := make([]*redis.Command, len(batch))
		for i := range batch {
			cmds[i] = batch[i].cmd
		}
		return []clientGroup{{client: first, cmds: cmds}}
	}

	index := make(map[client.RedisClient]int)
	var groups []clientGroup
	for _, it := range batch {
		i, ok := index[it.client]
		if !ok {
			i = len(groups)
			index[it.client] = i
			groups = append(groups, clientGroup{client: it.client})
		}
		groups[i].cmds = append(groups[i].cmds, it.cmd)
	}
	return groups
}

func (b *Bulk) executeGroups(groups []clientGroup) error {
	errs := make([]error, len(groups))
	var wg sync.WaitGroup
	wg.Add(len(groups))
	for i := range groups {
		go func(i int) {
			defer wg.Done()
			errs[i] = b.execute(groups[i].client, groups[i].cmds)
		}(i)
	}
	wg.Wait()
	return errors.Join(errs...)
}

// execute sends cmds as one pipeline and retries the commands that failed with a transient
// error. A retry re-sends the failed commands together with every later command of the same
// key, so a retried write can never overwrite a newer one. Like a DCP replay after a restart,
// this may apply a command more than once.
func (b *Bulk) execute(c client.RedisClient, cmds []*redis.Command) error {
	backoff := b.retryMinBackoff
	for attempt := 0; ; attempt++ {
		errs := b.execPipeline(c, cmds)

		var firstErr error
		failed := 0
		for i, err := range errs {
			if err == nil {
				continue
			}
			if !isRetryableError(err) {
				return fmt.Errorf("%s %q: %w", cmds[i].Operation, cmds[i].Key, err)
			}
			if firstErr == nil {
				firstErr = fmt.Errorf("%s %q: %w", cmds[i].Operation, cmds[i].Key, err)
			}
			failed++
		}
		if failed == 0 {
			return nil
		}
		if attempt >= b.maxRetries {
			return fmt.Errorf("giving up after %d retries: %w", attempt, firstErr)
		}

		cmds = retrySet(cmds, errs)
		b.retriedCommands.Add(int64(len(cmds)))
		logger.Log.Warn("retrying %d redis commands (%d failed, attempt %d/%d) in %v, err: %v",
			len(cmds), failed, attempt+1, b.maxRetries, backoff, firstErr)
		time.Sleep(backoff)
		backoff = min(backoff*2, b.retryMaxBackoff)
	}
}

// retrySet returns the failed commands plus every later command of a key that has a failed
// command, in their original order.
func retrySet(cmds []*redis.Command, errs []error) []*redis.Command {
	dirty := make(map[string]struct{})
	out := make([]*redis.Command, 0, len(cmds))
	for i, cmd := range cmds {
		if errs[i] != nil {
			dirty[cmd.Key] = struct{}{}
			out = append(out, cmd)
			continue
		}
		if _, ok := dirty[cmd.Key]; ok {
			out = append(out, cmd)
		}
	}
	return out
}

// execPipeline sends cmds in one pipeline and returns the error of each command (nil on
// success; a nil reply is not an error).
func (b *Bulk) execPipeline(c client.RedisClient, cmds []*redis.Command) []error {
	pipe := c.Pipeline()
	if !isClusterClient(c) {
		// A standalone/sentinel go-redis pipeline copies the error of its first command onto
		// every other command (and retries the whole pipeline for LOADING and friends). A
		// leading PING keeps the error of each command exact.
		pipe.Ping(b.ctx)
	}
	queued := 0
	results := make([]goredis.Cmder, len(cmds))
	for i, cmd := range cmds {
		results[i] = queueCommand(b.ctx, pipe, cmd)
		if results[i] != nil {
			queued++
		}
	}

	errs := make([]error, len(cmds))
	if queued == 0 {
		return errs
	}
	// Exec reports the first failed command; every command is inspected below instead.
	_, _ = pipe.Exec(b.ctx)

	for i, result := range results {
		if result == nil {
			continue
		}
		if err := result.Err(); err != nil && !errors.Is(err, goredis.Nil) {
			errs[i] = err
		}
	}
	return errs
}

// queueCommand adds cmd to the pipeline and returns the reply that decides whether the
// command succeeded, or nil when the command is skipped. SET, DEL, HSET and HDEL use their
// typed fields; any other operation (redis.Raw, e.g. HYRAT.SET or ZADD) is sent as
// "<Operation> <Key> <Args...>".
func queueCommand(ctx context.Context, pipe goredis.Pipeliner, cmd *redis.Command) goredis.Cmder {
	switch cmd.Operation {
	case "SET":
		return pipe.Set(ctx, cmd.Key, cmd.Value, cmd.TTL)
	case "DEL":
		return pipe.Del(ctx, cmd.Key)
	case "HSET":
		if len(cmd.Args) < 2 {
			return nil
		}
		result := pipe.HSet(ctx, cmd.Key, cmd.Args[0], cmd.Args[1])
		if cmd.TTL > 0 {
			pipe.Expire(ctx, cmd.Key, cmd.TTL)
		}
		return result
	case "HDEL":
		if len(cmd.Args) < 1 {
			return nil
		}
		return pipe.HDel(ctx, cmd.Key, toFieldString(cmd.Args[0]))
	default:
		args := make([]interface{}, 0, len(cmd.Args)+2)
		args = append(args, cmd.Operation, cmd.Key)
		args = append(args, cmd.Args...)
		return pipe.Do(ctx, args...)
	}
}

// isClusterClient reports whether c is a Redis Cluster client. Cluster pipelines read every
// node's replies independently and need no leading PING.
func isClusterClient(c client.RedisClient) bool {
	_, ok := c.(interface {
		ForEachShard(ctx context.Context, fn func(ctx context.Context, client *goredis.Client) error) error
	})
	return ok
}

func toFieldString(v interface{}) string {
	switch value := v.(type) {
	case string:
		return value
	case []byte:
		return string(value)
	default:
		return fmt.Sprint(value)
	}
}

func isRetryableError(err error) bool {
	if err == nil || errors.Is(err, goredis.ErrClosed) || errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, goredis.ErrPoolTimeout) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	var redisErr goredis.Error
	if errors.As(err, &redisErr) {
		msg := redisErr.Error()
		for _, prefix := range retryableErrorPrefixes {
			if msg == prefix || strings.HasPrefix(msg, prefix+" ") {
				return true
			}
		}
	}
	return false
}

var dbgN, dbgCmds, dbgNs atomic.Int64
