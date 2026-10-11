package bulk

import (
	"context"
	"fmt"
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
	instanceClients     map[string]client.RedisClient
	collectionClients   map[string]client.RedisClient
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

	instanceClients, err := newInstanceClients(cfg.Redis.Instances)
	if err != nil {
		c.Close()
		return nil, err
	}

	b := Bulk{
		redisClient:         c,
		instanceClients:     instanceClients,
		collectionClients:   buildCollectionClients(cfg.Redis.CollectionKeyMapping, instanceClients),
		dcpCheckpointCommit: dcpCheckpointCommit,
		batchTickerDuration: cfg.Redis.BatchTickerDuration,
		batchTicker:         time.NewTicker(cfg.Redis.BatchTickerDuration),
		metric:              &Metric{},
		ctx:                 context.Background(),
	}
	return &b, nil
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
	for _, c := range b.instanceClients {
		c.Close()
	}
}

func (b *Bulk) AddActions(ctx *models.ListenerContext, eventTime time.Time, collectionName string, actions []redis.Model) {
	b.flushLock.Lock()
	if b.isDcpRebalancing {
		logger.Log.Warn("could not add new message to batch while rebalancing")
		b.flushLock.Unlock()
		return
	}
	b.flushLock.Unlock()
	b.metric.ProcessLatencyMs = time.Since(eventTime).Milliseconds()
	b.flush(ctx, b.GetRedisClientByCollection(collectionName), actions)
}

func (b *Bulk) flush(ctx *models.ListenerContext, c client.RedisClient, models []redis.Model) {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()
	if b.isDcpRebalancing {
		return
	}

	startedTime := time.Now()
	for _, model := range models {
		command := model.Convert()
		err := b.executeCommand(c, command)
		if err != nil {
			logger.Log.Error("error while redis exec, err: %v", err)
			panic(err)
		}
	}
	b.metric.BulkRequestProcessLatencyMs = time.Since(startedTime).Milliseconds()
	ctx.Ack()
}

func (b *Bulk) executeCommand(c client.RedisClient, cmd *redis.Command) error {
	switch cmd.Operation {
	case "SET":
		err := c.Set(b.ctx, cmd.Key, cmd.Value, cmd.TTL).Err()
		return err
	case "DEL":
		err := c.Del(b.ctx, cmd.Key).Err()
		return err
	case "HSET":
		if len(cmd.Args) >= 2 {
			err := c.HSet(b.ctx, cmd.Key, cmd.Args[0], cmd.Args[1]).Err()
			if err == nil && cmd.TTL > 0 {
				c.Expire(b.ctx, cmd.Key, cmd.TTL)
			}
			return err
		}
	case "HDEL":
		if len(cmd.Args) >= 1 {
			err := c.HDel(b.ctx, cmd.Key, cmd.Args[0].(string)).Err()
			return err
		}
	default:
		args := make([]interface{}, 0)
		args = append(args, cmd.Operation)
		args = append(args, cmd.Key)
		args = append(args, cmd.Args...)
		return c.Do(b.ctx, args...).Err()
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

// GetRedisClientByCollection returns the collection's target client, or the default one.
func (b *Bulk) GetRedisClientByCollection(collectionName string) client.RedisClient {
	if c, ok := b.collectionClients[collectionName]; ok {
		return c
	}
	return b.redisClient
}
