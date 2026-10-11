package integration

import (
	"testing"

	"github.com/Trendyol/go-dcp-redis/config"
)

// _default is routed to "secondary" (6380); the default Redis (6379) must stay empty.
func TestRedisMultiInstance(t *testing.T) {
	testRedisConnection(t, "config-multi.yml", config.Redis{
		Host: "localhost",
		Port: 6380,
		DB:   2, // Use different database to avoid conflicts
	})

	assertNoKeys(t, config.Redis{
		Host: "localhost",
		Port: 6379,
		DB:   2,
	}, "doc:*")
}
