package bulk

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Trendyol/go-dcp-redis/config"
	"github.com/Trendyol/go-dcp-redis/redis"
	"github.com/Trendyol/go-dcp-redis/redis/client"
	"github.com/Trendyol/go-dcp/logger"
	"github.com/Trendyol/go-dcp/models"
)

const (
	defaultRetryMinBackoff = 100 * time.Millisecond
	defaultRetryMaxBackoff = 3 * time.Second
)

// Bulk buffers the commands produced by the mapper and writes them to Redis in pipelines.
//
// DCP listeners only append to the pending batch and return, so consuming DCP events is not
// bound to one Redis round trip per event. A single flusher goroutine sends everything that
// is pending as one pipeline per Redis client while new events keep accumulating, so batches
// grow with the load and an event is written as soon as the previous pipeline completes.
// Events are acknowledged when they are buffered; the checkpoint is only committed after
// every acknowledged event has been written, so a crash replays (never skips) events.
type Bulk struct {
	ctx                 context.Context
	redisClient         client.RedisClient
	instanceClients     map[string]client.RedisClient
	collectionClients   map[string]client.RedisClient
	dcpCheckpointCommit func()
	fatal               func(error)
	batchTicker         *time.Ticker
	cond                *sync.Cond
	tickerStop          chan struct{}
	flusherDone         chan struct{}
	pending             []item
	spare               []item

	processLatencyMs            atomic.Int64
	bulkRequestProcessLatencyMs atomic.Int64
	bulkRequestSize             atomic.Int64
	retriedCommands             atomic.Int64

	mu                 sync.Mutex
	closeOnce          sync.Once
	pendingBytes       int
	batchSizeLimit     int
	batchByteSizeLimit int
	maxRetries         int
	retryMinBackoff    time.Duration
	retryMaxBackoff    time.Duration
	inflight           bool
	committing         bool
	isDcpRebalancing   bool
	closed             bool
	started            bool
}

type item struct {
	client client.RedisClient
	cmd    *redis.Command
}

func NewBulk(
	cfg *config.Connector,
	dcpCheckpointCommit func(),
) (*Bulk, error) {
	if err := cfg.Redis.Validate(); err != nil {
		return nil, err
	}

	c, err := client.NewRedisClient(cfg.Redis)
	if err != nil {
		return nil, err
	}

	instanceClients, err := newInstanceClients(cfg.Redis.Instances)
	if err != nil {
		c.Close()
		return nil, err
	}

	b := newBulk(cfg.Redis, c, instanceClients, dcpCheckpointCommit)
	go b.runFlusher()
	return b, nil
}

// newBulk builds a Bulk without connecting or starting the flusher goroutine.
func newBulk(
	cfg config.Redis,
	c client.RedisClient,
	instanceClients map[string]client.RedisClient,
	dcpCheckpointCommit func(),
) *Bulk {
	tickerDuration := cfg.BatchTickerDuration
	if tickerDuration <= 0 {
		tickerDuration = config.DefaultBatchTickerDuration
	}
	batchSizeLimit := cfg.BatchSizeLimit
	if batchSizeLimit <= 0 {
		batchSizeLimit = config.DefaultBatchSizeLimit
	}
	maxRetries := cfg.MaxRetries
	switch {
	case maxRetries == 0:
		maxRetries = config.DefaultMaxRetries
	case maxRetries < 0:
		maxRetries = 0
	}

	b := &Bulk{
		ctx:                 context.Background(),
		redisClient:         c,
		instanceClients:     instanceClients,
		collectionClients:   buildCollectionClients(cfg.CollectionKeyMapping, instanceClients),
		dcpCheckpointCommit: dcpCheckpointCommit,
		fatal:               defaultFatal,
		batchTicker:         time.NewTicker(tickerDuration),
		tickerStop:          make(chan struct{}),
		flusherDone:         make(chan struct{}),
		batchSizeLimit:      batchSizeLimit,
		batchByteSizeLimit:  cfg.BatchByteSizeLimitBytes(),
		maxRetries:          maxRetries,
		retryMinBackoff:     defaultRetryMinBackoff,
		retryMaxBackoff:     defaultRetryMaxBackoff,
	}
	b.cond = sync.NewCond(&b.mu)
	return b
}

func defaultFatal(err error) {
	logger.Log.Error("error while redis exec, err: %v", err)
	panic(err)
}

func newInstanceClients(instances map[string]config.RedisInstance) (map[string]client.RedisClient, error) {
	clients := make(map[string]client.RedisClient, len(instances))
	for name, instance := range instances {
		c, err := client.NewInstanceClient(instance)
		if err != nil {
			for _, opened := range clients {
				opened.Close()
			}
			return nil, fmt.Errorf("redis instance %q: %w", name, err)
		}
		clients[name] = c
	}
	return clients, nil
}

// buildCollectionClients maps targeted collections to their instance clients.
func buildCollectionClients(
	mappings []config.CollectionKeyMapping,
	instanceClients map[string]client.RedisClient,
) map[string]client.RedisClient {
	clients := make(map[string]client.RedisClient)
	for _, mapping := range mappings {
		if mapping.Target == "" {
			continue
		}
		if c, ok := instanceClients[mapping.Target]; ok {
			clients[mapping.Collection] = c
		}
	}
	return clients
}

type Metric struct {
	ProcessLatencyMs            int64
	BulkRequestProcessLatencyMs int64
	// BulkRequestSize is the number of commands in the last pipeline sent to Redis.
	BulkRequestSize int64
	// RetriedCommands is the total number of commands re-sent after a transient error.
	RetriedCommands int64
}

// GetMetric returns a snapshot of the bulk metrics.
func (b *Bulk) GetMetric() *Metric {
	return &Metric{
		ProcessLatencyMs:            b.processLatencyMs.Load(),
		BulkRequestProcessLatencyMs: b.bulkRequestProcessLatencyMs.Load(),
		BulkRequestSize:             b.bulkRequestSize.Load(),
		RetriedCommands:             b.retriedCommands.Load(),
	}
}

// StartBulk commits the DCP checkpoint every batchTickerDuration until Close is called.
func (b *Bulk) StartBulk() {
	b.mu.Lock()
	b.started = true
	b.mu.Unlock()

	for {
		select {
		case <-b.tickerStop:
			return
		case <-b.batchTicker.C:
			b.commit()
		}
	}
}

// commit saves the checkpoint once every acknowledged event has been written to Redis.
// New events wait while the pending batch drains, so the checkpoint never covers an event
// that is only buffered.
func (b *Bulk) commit() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.isDcpRebalancing || b.closed {
		return
	}

	b.committing = true
	for (len(b.pending) > 0 || b.inflight) && !b.isDcpRebalancing && !b.closed {
		b.cond.Wait()
	}
	if !b.isDcpRebalancing && !b.closed && b.dcpCheckpointCommit != nil {
		b.dcpCheckpointCommit()
	}
	b.committing = false
	b.cond.Broadcast()
}

// Close writes every pending command, then stops the bulk and closes the Redis clients.
func (b *Bulk) Close() {
	b.closeOnce.Do(func() {
		b.mu.Lock()
		b.isDcpRebalancing = true
		b.closed = true
		b.cond.Broadcast()
		b.mu.Unlock()

		<-b.flusherDone

		b.batchTicker.Stop()
		close(b.tickerStop)

		if b.redisClient != nil {
			b.redisClient.Close()
		}
		for _, c := range b.instanceClients {
			c.Close()
		}
	})
}

// AddActions buffers the commands of one DCP event and acknowledges the event. It blocks
// while the pending batch is full (batchSizeLimit / batchByteSizeLimit) or a checkpoint
// commit is draining it, which applies back-pressure to the DCP stream.
func (b *Bulk) AddActions(ctx *models.ListenerContext, eventTime time.Time, collectionName string, actions []redis.Model) {
	c := b.GetRedisClientByCollection(collectionName)
	items := make([]item, 0, len(actions))
	size := 0
	for _, action := range actions {
		if action == nil {
			continue
		}
		cmd := action.Convert()
		if cmd == nil {
			continue
		}
		items = append(items, item{client: c, cmd: cmd})
		size += estimateSize(cmd)
	}

	b.mu.Lock()
	for !b.isDcpRebalancing && (b.committing || b.isFullLocked(len(items), size)) {
		b.cond.Wait()
	}
	if b.isDcpRebalancing {
		b.mu.Unlock()
		logger.Log.Warn("could not add new message to batch while rebalancing")
		return
	}
	b.pending = append(b.pending, items...)
	b.pendingBytes += size
	ctx.Ack()
	b.cond.Broadcast()
	b.mu.Unlock()

	b.processLatencyMs.Store(time.Since(eventTime).Milliseconds())
}

// isFullLocked reports whether n more commands of the given size would overflow the pending
// batch. An empty batch always accepts an event, however large.
func (b *Bulk) isFullLocked(n, size int) bool {
	if len(b.pending) == 0 {
		return false
	}
	return len(b.pending)+n > b.batchSizeLimit || b.pendingBytes+size > b.batchByteSizeLimit
}

// waitDrainedLocked blocks until every buffered command has been written.
func (b *Bulk) waitDrainedLocked() {
	for len(b.pending) > 0 || b.inflight {
		b.cond.Wait()
	}
}

// runFlusher writes the pending batch whenever there is one, until Close drains it.
func (b *Bulk) runFlusher() {
	defer close(b.flusherDone)
	for {
		b.mu.Lock()
		for len(b.pending) == 0 && !b.closed {
			b.cond.Wait()
		}
		if len(b.pending) == 0 {
			b.mu.Unlock()
			return
		}
		batch := b.pending
		b.pending = b.spare[:0]
		b.spare = nil
		b.pendingBytes = 0
		b.inflight = true
		b.cond.Broadcast()
		b.mu.Unlock()

		if err := b.flushBatch(batch); err != nil {
			b.fatal(err)
		}

		clear(batch)
		b.mu.Lock()
		b.spare = batch[:0]
		b.inflight = false
		b.cond.Broadcast()
		b.mu.Unlock()
	}
}

func (b *Bulk) PrepareStartRebalancing() {
	b.mu.Lock()
	b.isDcpRebalancing = true
	b.cond.Broadcast()
	b.waitDrainedLocked()
	started := b.started
	b.mu.Unlock()

	// Every acknowledged event is in Redis now, so saving the checkpoint before the streams
	// stop spares the next owner of these vBuckets from replaying them.
	if started && b.dcpCheckpointCommit != nil {
		b.dcpCheckpointCommit()
	}
}

func (b *Bulk) PrepareEndRebalancing() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return
	}
	b.isDcpRebalancing = false
	b.cond.Broadcast()
}

func (b *Bulk) GetRedisClient() client.RedisClient {
	return b.redisClient
}

// GetRedisClientByCollection returns the collection's target client, or the default one.
func (b *Bulk) GetRedisClientByCollection(collectionName string) client.RedisClient {
	if c, ok := b.collectionClients[collectionName]; ok {
		return c
	}
	return b.redisClient
}

func estimateSize(cmd *redis.Command) int {
	size := len(cmd.Operation) + len(cmd.Key) + valueSize(cmd.Value)
	for _, arg := range cmd.Args {
		size += valueSize(arg)
	}
	return size
}

func valueSize(v interface{}) int {
	switch value := v.(type) {
	case nil:
		return 0
	case string:
		return len(value)
	case []byte:
		return len(value)
	default:
		return 8
	}
}
