package dcpredis

import (
	"testing"

	"github.com/Trendyol/go-dcp-redis/config"
)

func TestMaskInstance(t *testing.T) {
	original := config.RedisInstance{
		Host:     "localhost",
		Password: "top-secret",
		Sentinel: &config.RedisSentinel{MasterName: "mymaster", Password: "sentinel-secret"},
		Cluster:  &config.RedisCluster{Addrs: []string{"localhost:7000"}, Password: "cluster-secret"},
	}

	masked := maskInstance(original)

	if masked.Password != maskedPassword {
		t.Errorf("Password = %q, want masked", masked.Password)
	}
	if masked.Sentinel.Password != maskedPassword {
		t.Errorf("Sentinel.Password = %q, want masked", masked.Sentinel.Password)
	}
	if masked.Cluster.Password != maskedPassword {
		t.Errorf("Cluster.Password = %q, want masked", masked.Cluster.Password)
	}
	if masked.Host != original.Host || masked.Sentinel.MasterName != original.Sentinel.MasterName {
		t.Error("non-secret fields must be preserved")
	}

	if original.Password != "top-secret" ||
		original.Sentinel.Password != "sentinel-secret" ||
		original.Cluster.Password != "cluster-secret" {
		t.Error("maskInstance mutated the original config")
	}
}

func TestMaskInstanceWithoutSentinelOrCluster(t *testing.T) {
	masked := maskInstance(config.RedisInstance{Host: "localhost", Password: "secret"})

	if masked.Password != maskedPassword {
		t.Errorf("Password = %q, want masked", masked.Password)
	}
	if masked.Sentinel != nil || masked.Cluster != nil {
		t.Error("nil sentinel/cluster must stay nil")
	}
}
