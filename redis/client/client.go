package client

import (
	"context"
	"fmt"

	"github.com/Trendyol/go-dcp-redis/config"
	"github.com/redis/go-redis/v9"
)

// RedisClient is an interface that covers both single and cluster Redis clients
type RedisClient interface {
	redis.Cmdable
	Close() error
	Do(ctx context.Context, args ...interface{}) *redis.Cmd
	Process(ctx context.Context, cmd redis.Cmder) error
}

// clientWrapper wraps a *redis.Client to implement RedisClient
type clientWrapper struct {
	*redis.Client
}

func (c *clientWrapper) Close() error {
	return c.Client.Close()
}

func (c *clientWrapper) Do(ctx context.Context, args ...interface{}) *redis.Cmd {
	return c.Client.Do(ctx, args...)
}

func (c *clientWrapper) Process(ctx context.Context, cmd redis.Cmder) error {
	return c.Client.Process(ctx, cmd)
}

// clusterWrapper wraps a *redis.ClusterClient to implement RedisClient
type clusterWrapper struct {
	*redis.ClusterClient
}

func (c *clusterWrapper) Close() error {
	return c.ClusterClient.Close()
}

func (c *clusterWrapper) Do(ctx context.Context, args ...interface{}) *redis.Cmd {
	return c.ClusterClient.Do(ctx, args...)
}

func (c *clusterWrapper) Process(ctx context.Context, cmd redis.Cmder) error {
	return c.ClusterClient.Process(ctx, cmd)
}

func NewRedisClient(cfg config.Redis) (RedisClient, error) {
	var client RedisClient

	// Check configuration type
	switch {
	case cfg.Cluster != nil && len(cfg.Cluster.Addrs) > 0:
		client = newClusterClient(cfg)
	case cfg.Sentinel != nil && len(cfg.Sentinel.SentinelAddrs) > 0:
		client = newSentinelClient(cfg)
	default:
		client = newStandaloneClient(cfg)
	}

	// Test connection
	ctx := context.Background()
	_, err := client.Ping(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return client, nil
}

func newStandaloneClient(cfg config.Redis) RedisClient {
	options := &redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Password: cfg.Password,
		DB:       cfg.DB,
	}

	if cfg.Username != "" {
		options.Username = cfg.Username
	}

	return &clientWrapper{redis.NewClient(options)}
}

func newSentinelClient(cfg config.Redis) RedisClient {
	options := &redis.FailoverOptions{
		MasterName:    cfg.Sentinel.MasterName,
		SentinelAddrs: cfg.Sentinel.SentinelAddrs,
		DB:            cfg.DB,
	}

	// Use Redis config credentials if Sentinel specific ones are not provided
	if cfg.Sentinel.Username != "" {
		options.Username = cfg.Sentinel.Username
	} else if cfg.Username != "" {
		options.Username = cfg.Username
	}

	if cfg.Sentinel.Password != "" {
		options.Password = cfg.Sentinel.Password
	} else if cfg.Password != "" {
		options.Password = cfg.Password
	}

	return &clientWrapper{redis.NewFailoverClient(options)}
}

func newClusterClient(cfg config.Redis) RedisClient {
	options := &redis.ClusterOptions{
		Addrs: cfg.Cluster.Addrs,
	}

	// Use Cluster specific credentials if provided, otherwise fall back to Redis config
	if cfg.Cluster.Username != "" {
		options.Username = cfg.Cluster.Username
	} else if cfg.Username != "" {
		options.Username = cfg.Username
	}

	if cfg.Cluster.Password != "" {
		options.Password = cfg.Cluster.Password
	} else if cfg.Password != "" {
		options.Password = cfg.Password
	}

	// Set routing options
	options.RouteByLatency = cfg.Cluster.RouteByLatency
	options.RouteRandomly = cfg.Cluster.RouteRandomly
	options.ReadOnly = cfg.Cluster.ReadOnly

	clusterClient := redis.NewClusterClient(options)

	return &clusterWrapper{clusterClient}
}
