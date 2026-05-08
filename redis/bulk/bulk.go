package bulk

import (
	"context"
	"sync"
	"time"

	"github.com/Trendyol/go-dcp-redis/config"
	"github.com/Trendyol/go-dcp-redis/redis"
	"github.com/Trendyol/go-dcp-redis/redis/client"
	"github.com/Trendyol/go-dcp/logger"
	"github.com/Trendyol/go-dcp/models"
)

type Bulk struct {
	redisClient         client.RedisClient
	dcpCheckpointCommit func()
	batchTicker         *time.Ticker
	metric              *Metric
	batchTickerDuration time.Duration
	flushLock           sync.Mutex
	isDcpRebalancing    bool
	ctx                 context.Context
}

func NewBulk(
	cfg *config.Connector,
	dcpCheckpointCommit func(),
) (*Bulk, error) {
	c, err := client.NewRedisClient(cfg.Redis)
	if err != nil {
		return nil, err
	}

	b := Bulk{
		redisClient:         c,
		dcpCheckpointCommit: dcpCheckpointCommit,
		batchTickerDuration: cfg.Redis.BatchTickerDuration,
		batchTicker:         time.NewTicker(cfg.Redis.BatchTickerDuration),
		metric:              &Metric{},
		ctx:                 context.Background(),
	}
	return &b, nil
}

type Metric struct {
	ProcessLatencyMs            int64
	BulkRequestProcessLatencyMs int64
}

func (b *Bulk) GetMetric() *Metric {
	return b.metric
}

func (b *Bulk) StartBulk() {
	for range b.batchTicker.C {
		b.dcpCheckpointCommit()
	}
}

func (b *Bulk) Close() {
	b.flushLock.Lock()
	b.isDcpRebalancing = true
	b.flushLock.Unlock()

	b.batchTicker.Stop()
	if b.redisClient != nil {
		b.redisClient.Close()
	}
}

func (b *Bulk) AddActions(ctx *models.ListenerContext, eventTime time.Time, actions []redis.Model) {
	b.flushLock.Lock()
	if b.isDcpRebalancing {
		logger.Log.Warn("could not add new message to batch while rebalancing")
		b.flushLock.Unlock()
		return
	}
	b.flushLock.Unlock()
	b.metric.ProcessLatencyMs = time.Since(eventTime).Milliseconds()
	b.flush(ctx, actions)
}

func (b *Bulk) flush(ctx *models.ListenerContext, models []redis.Model) {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()
	if b.isDcpRebalancing {
		return
	}

	startedTime := time.Now()
	for _, model := range models {
		command := model.Convert()
		err := b.executeCommand(command)
		if err != nil {
			logger.Log.Error("error while redis exec, err: %v", err)
			panic(err)
		}
	}
	b.metric.BulkRequestProcessLatencyMs = time.Since(startedTime).Milliseconds()
	ctx.Ack()
}

func (b *Bulk) executeCommand(cmd *redis.Command) error {
	switch cmd.Operation {
	case "SET":
		err := b.redisClient.Set(b.ctx, cmd.Key, cmd.Value, cmd.TTL).Err()
		return err
	case "DEL":
		err := b.redisClient.Del(b.ctx, cmd.Key).Err()
		return err
	case "HSET":
		if len(cmd.Args) >= 2 {
			err := b.redisClient.HSet(b.ctx, cmd.Key, cmd.Args[0], cmd.Args[1]).Err()
			if err == nil && cmd.TTL > 0 {
				b.redisClient.Expire(b.ctx, cmd.Key, cmd.TTL)
			}
			return err
		}
	case "HDEL":
		if len(cmd.Args) >= 1 {
			err := b.redisClient.HDel(b.ctx, cmd.Key, cmd.Args[0].(string)).Err()
			return err
		}
	default:
		args := make([]interface{}, 0)
		args = append(args, cmd.Operation)
		args = append(args, cmd.Key)
		args = append(args, cmd.Args...)
		return b.redisClient.Do(b.ctx, args...).Err()
	}
	return nil
}

func (b *Bulk) PrepareStartRebalancing() {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()

	b.isDcpRebalancing = true
}

func (b *Bulk) PrepareEndRebalancing() {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()

	b.isDcpRebalancing = false
}

func (b *Bulk) GetRedisClient() client.RedisClient {
	return b.redisClient
}
