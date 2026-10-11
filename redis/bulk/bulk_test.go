package bulk

import (
	"strings"
	"testing"

	"github.com/Trendyol/go-dcp-redis/config"
	"github.com/Trendyol/go-dcp-redis/redis/client"
	goredis "github.com/redis/go-redis/v9"
)

// lazyClient never connects; go-redis dials on the first command.
func lazyClient() client.RedisClient {
	return goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1"})
}

func TestBuildCollectionClients(t *testing.T) {
	eu := lazyClient()
	us := lazyClient()
	instanceClients := map[string]client.RedisClient{"eu": eu, "us": us}

	mappings := []config.CollectionKeyMapping{
		{Collection: "products", Target: "eu"},
		{Collection: "orders", Target: "us"},
		{Collection: "users"},
	}

	got := buildCollectionClients(mappings, instanceClients)

	if got["products"] != eu {
		t.Errorf("products routed to %v, want eu client", got["products"])
	}
	if got["orders"] != us {
		t.Errorf("orders routed to %v, want us client", got["orders"])
	}
	if _, ok := got["users"]; ok {
		t.Error("users has no target and must not be routed")
	}
}

func TestGetRedisClientByCollection(t *testing.T) {
	def := lazyClient()
	eu := lazyClient()
	b := &Bulk{
		redisClient:       def,
		collectionClients: map[string]client.RedisClient{"products": eu},
	}

	if got := b.GetRedisClientByCollection("products"); got != eu {
		t.Errorf("products client = %v, want eu client", got)
	}
	if got := b.GetRedisClientByCollection("users"); got != def {
		t.Errorf("users client = %v, want default client", got)
	}
}

func TestNewBulkRejectsUnknownTarget(t *testing.T) {
	cfg := &config.Connector{Redis: config.Redis{
		CollectionKeyMapping: []config.CollectionKeyMapping{{Collection: "products", Target: "nope"}},
	}}

	_, err := NewBulk(cfg, nil)
	if err == nil || !strings.Contains(err.Error(), `unknown redis target "nope"`) {
		t.Fatalf("err = %v, want unknown target error", err)
	}
}

func TestNewInstanceClientsEmpty(t *testing.T) {
	clients, err := newInstanceClients(nil)
	if err != nil {
		t.Fatalf("newInstanceClients(nil) = %v, want nil", err)
	}
	if len(clients) != 0 {
		t.Errorf("got %d clients, want 0", len(clients))
	}
}

func TestNewInstanceClientsReportsInstanceName(t *testing.T) {
	_, err := newInstanceClients(map[string]config.RedisInstance{
		"unreachable": {Host: "127.0.0.1", Port: 1},
	})
	if err == nil || !strings.Contains(err.Error(), `redis instance "unreachable"`) {
		t.Fatalf("err = %v, want the instance name in the error", err)
	}
}
