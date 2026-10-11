package config

import (
	"fmt"
	"time"

	"github.com/Trendyol/go-dcp/config"
)

type Redis struct {
	Host                 string                   `yaml:"host"`
	Port                 uint16                   `yaml:"port"`
	Username             string                   `yaml:"username"`
	Password             string                   `yaml:"password"`
	DB                   int                      `yaml:"db"`
	CollectionKeyMapping []CollectionKeyMapping   `yaml:"collectionKeyMapping,omitempty"`
	BatchTickerDuration  time.Duration            `yaml:"batchTickerDuration"`
	DefaultTTL           time.Duration            `yaml:"defaultTTL"`
	Sentinel             *RedisSentinel           `yaml:"sentinel,omitempty"`
	Cluster              *RedisCluster            `yaml:"cluster,omitempty"`
	Instances            map[string]RedisInstance `yaml:"instances,omitempty"`
}

// RedisInstance is the connection config of one Redis (standalone, sentinel or cluster).
type RedisInstance struct {
	Host     string         `yaml:"host"`
	Port     uint16         `yaml:"port"`
	Username string         `yaml:"username"`
	Password string         `yaml:"password"`
	DB       int            `yaml:"db"`
	Sentinel *RedisSentinel `yaml:"sentinel,omitempty"`
	Cluster  *RedisCluster  `yaml:"cluster,omitempty"`
}

// DefaultInstance returns the top-level redis block as an instance.
func (r *Redis) DefaultInstance() RedisInstance {
	return RedisInstance{
		Host:     r.Host,
		Port:     r.Port,
		Username: r.Username,
		Password: r.Password,
		DB:       r.DB,
		Sentinel: r.Sentinel,
		Cluster:  r.Cluster,
	}
}

// Validate checks that instances have an address and collection targets refer to defined instances.
func (r *Redis) Validate() error {
	for name, instance := range r.Instances {
		if !instance.hasAddress() {
			return fmt.Errorf("redis instance %q: host, sentinel or cluster is required", name)
		}
	}
	for _, mapping := range r.CollectionKeyMapping {
		if mapping.Target == "" {
			continue
		}
		if _, ok := r.Instances[mapping.Target]; !ok {
			return fmt.Errorf("collection %q: unknown redis target %q", mapping.Collection, mapping.Target)
		}
	}
	return nil
}

func (i RedisInstance) hasAddress() bool {
	return i.Host != "" ||
		(i.Sentinel != nil && len(i.Sentinel.SentinelAddrs) > 0) ||
		(i.Cluster != nil && len(i.Cluster.Addrs) > 0)
}

type RedisSentinel struct {
	MasterName    string   `yaml:"masterName"`
	SentinelAddrs []string `yaml:"sentinelAddrs"`
	Username      string   `yaml:"username,omitempty"`
	Password      string   `yaml:"password,omitempty"`
}

type RedisCluster struct {
	Addrs          []string `yaml:"addrs"`
	Username       string   `yaml:"username,omitempty"`
	Password       string   `yaml:"password,omitempty"`
	RouteByLatency bool     `yaml:"routeByLatency,omitempty"`
	RouteRandomly  bool     `yaml:"routeRandomly,omitempty"`
	ReadOnly       bool     `yaml:"readOnly,omitempty"`
}

type CollectionKeyMapping struct {
	Collection  string        `yaml:"collection"`
	KeyPrefix   string        `yaml:"keyPrefix"`
	KeySuffix   string        `yaml:"keySuffix"`
	StorageType string        `yaml:"storageType"` // "string", "hash", "json"
	TTL         time.Duration `yaml:"ttl"`
	HashField   string        `yaml:"hashField,omitempty"`
	Target      string        `yaml:"target,omitempty"` // redis.instances name, empty means default
}

type Connector struct {
	Redis Redis      `yaml:"redis" mapstructure:"redis"`
	Dcp   config.Dcp `yaml:",inline" mapstructure:",squash"`
}

func (c *Connector) ApplyDefaults() {
	if c.Redis.Port == 0 {
		c.Redis.Port = 6379
	}

	if c.Redis.BatchTickerDuration == 0 {
		c.Redis.BatchTickerDuration = 10 * time.Second
	}

	if c.Redis.DB == 0 {
		c.Redis.DB = 0
	}

	for name, instance := range c.Redis.Instances {
		if instance.Port == 0 {
			instance.Port = 6379
			c.Redis.Instances[name] = instance
		}
	}

	// Set default storage type for mappings
	for i := range c.Redis.CollectionKeyMapping {
		if c.Redis.CollectionKeyMapping[i].StorageType == "" {
			c.Redis.CollectionKeyMapping[i].StorageType = "string"
		}
	}
}
