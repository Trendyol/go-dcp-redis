package integration

import (
	"context"
	"testing"
	"time"

	dcpredis "github.com/Trendyol/go-dcp-redis"
	"github.com/Trendyol/go-dcp-redis/config"
	"github.com/Trendyol/go-dcp-redis/redis/client"
	"github.com/Trendyol/go-dcp/logger"
)

func testRedisConnection(t *testing.T, configFile string, redisConfig config.Redis) {
	time.Sleep(time.Second * 30)

	connector, err := dcpredis.NewConnectorBuilder(configFile).Build()
	if err != nil {
		t.Fatal(err)
		return
	}

	// Start connector in a goroutine
	go connector.Start()

	// Wait a bit for connector to initialize
	time.Sleep(2 * time.Second)

	// Create Redis client for testing
	redisClient, err := client.NewRedisClient(redisConfig)
	if err != nil {
		t.Fatalf("could not open connection to redis %s", err)
	}

	// Ensure cleanup happens even if test fails
	defer func() {
		logger.Log.Info("starting cleanup process")

		// Close the connector and wait for all streams to stop
		connector.Close()
		logger.Log.Info("connector closed, waiting for all operations to complete...")

		// Wait longer for all DCP streams and Redis operations to fully complete
		time.Sleep(10 * time.Second)

		logger.Log.Info("cleanup completed")
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Check for keys periodically
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("deadline exceeded - not enough keys found")
		default:
			// Check if keys exist in Redis
			keys, err := redisClient.Keys(ctx, "doc:*").Result()
			if err != nil {
				t.Fatalf("redis query error %s", err)
			}

			logger.Log.Info("found %d keys", len(keys))

			if len(keys) >= 100 { // Check for at least 100 keys
				logger.Log.Info("test completed successfully - found %d keys", len(keys))

				// Wait a bit more to let any pending DCP events finish processing
				logger.Log.Info("waiting for pending events to finish...")
				time.Sleep(5 * time.Second)

				return // defer will handle cleanup
			}
			time.Sleep(2 * time.Second)
		}
	}
}

func assertNoKeys(t *testing.T, redisConfig config.Redis, pattern string) {
	redisClient, err := client.NewRedisClient(redisConfig)
	if err != nil {
		t.Fatalf("could not open connection to redis %s", err)
	}
	defer redisClient.Close()

	keys, err := redisClient.Keys(context.Background(), pattern).Result()
	if err != nil {
		t.Fatalf("redis query error %s", err)
	}

	if len(keys) != 0 {
		t.Fatalf("expected no %q keys, found %d", pattern, len(keys))
	}
}
