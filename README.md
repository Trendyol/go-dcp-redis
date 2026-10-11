# Go Dcp Redis [![Go Reference](https://pkg.go.dev/badge/github.com/Trendyol/go-dcp-redis.svg)](https://pkg.go.dev/github.com/Trendyol/go-dcp-redis) [![Go Report Card](https://goreportcard.com/badge/github.com/Trendyol/go-dcp-redis)](https://goreportcard.com/report/github.com/Trendyol/go-dcp-redis) [![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/Trendyol/go-dcp-redis/badge)](https://scorecard.dev/viewer/?uri=github.com/Trendyol/go-dcp-redis)

**Go Dcp Redis** streams documents from Couchbase Database Change Protocol (DCP) and writes to
Redis in near real-time.

## Features

- Custom Redis commands **per** DCP event.
- **Multiple Redis operations** for a DCP event(see [Example](#example)).
- Handling different DCP events such as **expiration, deletion and mutation**(see [Example](#example)).
- **Managing batch configurations** such as batch ticker durations.
- **Multiple storage types** (string, hash, json) with TTL support.
- **Redis Sentinel support** for high availability deployments.
- **Redis Cluster support** for horizontal scaling and high availability.
- **Multiple Redis instances**: route each collection to a different Redis (standalone, Sentinel or Cluster) from config.
- **Scale up and down** by custom membership algorithms(Couchbase, KubernetesHa, Kubernetes StatefulSet or
  Static, see [examples](https://github.com/Trendyol/go-dcp#examples)).
- **Easily manageable configurations**.

## Example

**Note:** If you prefer to use the default mapper by entering the configuration instead of creating a custom mapper, please refer to [this](#collection-key-mapping-configuration) topic.
Otherwise, you can refer to the example provided below:

```go
package main

import (
  "fmt"
  "time"
  dcpredis "github.com/Trendyol/go-dcp-redis"
  "github.com/Trendyol/go-dcp-redis/couchbase"
  "github.com/Trendyol/go-dcp-redis/redis"
)

func mapper(event couchbase.Context) []redis.Model {
  var set = redis.Set{
    Key:   fmt.Sprintf("doc:%s", string(event.Event.Key)),
    Value: string(event.Event.Value),
    TTL:   time.Hour * 24, // 24 hours TTL
  }

  return []redis.Model{&set}
}

func main() {
  connector, err := dcpredis.NewConnectorBuilder("config.yml").
    SetMapper(mapper). // NOT NEEDED IF YOU'RE USING DEFAULT MAPPER. JUST CALL Build() FUNCTION
    Build()
  if err != nil {
    panic(err)
  }

  defer connector.Close()
  connector.Start()
}
```

### Advanced Redis Usage Examples

**Hash Storage Example:**

```go
func hashMapper(event couchbase.Context) []redis.Model {
  return []redis.Model{
    &redis.HSet{
      Key:   fmt.Sprintf("user:%s", string(event.Event.Key)),
      Field: "profile",
      Value: string(event.Event.Value),
      TTL:   time.Hour * 48,
    },
  }
}
```

**Multiple Operations Example:**

```go
func multiMapper(event couchbase.Context) []redis.Model {
  key := string(event.Event.Key)
  value := string(event.Event.Value)

  return []redis.Model{
    // Store as string
    &redis.Set{
      Key:   fmt.Sprintf("doc:%s", key),
      Value: value,
      TTL:   time.Hour * 24,
    },
    // Store in hash for indexing
    &redis.HSet{
      Key:   "docs:index",
      Field: key,
      Value: time.Now().Unix(),
    },
  }
}
```

**Raw Command Example:**

```go
func rawMapper(event couchbase.Context) []redis.Model {
  return []redis.Model{
    &redis.Raw{
      Operation: "ZADD",
      Key:       "docs:timeline",
      Args:      []interface{}{time.Now().Unix(), string(event.Event.Key)},
    },
  }
}
```

## Configuration Examples

### Standalone Redis Configuration

```yaml
redis:
  host: localhost
  port: 6379
  password: ""
  db: 0
```

### Redis Sentinel Configuration

```yaml
redis:
  db: 0
  sentinel:
    masterName: mymaster
    sentinelAddrs:
      - localhost:26379
      - localhost:26380
      - localhost:26381
    password: ""
```

### Redis Cluster Configuration

```yaml
redis:
  cluster:
    addrs:
      - localhost:7000
      - localhost:7001
      - localhost:7002
      - localhost:7003
      - localhost:7004
      - localhost:7005
    routeByLatency: false
    routeRandomly: false
    readOnly: false
```

### Multiple Redis Instances Configuration

The top-level `redis` block is the default instance. Additional named instances go under `redis.instances`, and a collection is routed to one of them with `collectionKeyMapping[].target`. Collections without a `target` keep writing to the default instance.

```yaml
redis:
  host: localhost            # default instance
  port: 6379
  instances:
    cache-eu:
      host: redis-eu
      port: 6379
      username: eu-user
      password: eu-pass
    cache-cluster:
      cluster:
        addrs:
          - redis-c-1:7000
          - redis-c-2:7001
        password: cluster-pass
  collectionKeyMapping:
    - collection: products
      keyPrefix: "product:"
      target: cache-eu       # -> cache-eu
    - collection: orders
      keyPrefix: "order:"
      target: cache-cluster  # -> cache-cluster
    - collection: users
      keyPrefix: "user:"     # no target -> default instance
```

Each instance uses only its own credentials; nothing is inherited from the default instance. An unknown `target`, or an instance without `host`, `sentinel` or `cluster`, fails at startup, before any connection is opened.

Routing also applies to custom mappers: add a mapping entry with just `collection` and `target`, and `event.RedisClient` inside the mapper is the client of that instance.

To keep passwords out of the YAML file, load the config yourself and fill them in before building the connector:

```go
file, _ := os.ReadFile("config.yml")
var cfg config.Connector
_ = yaml.Unmarshal(file, &cfg)

instance := cfg.Redis.Instances["cache-eu"]
instance.Password = os.Getenv("CACHE_EU_PASSWORD")
cfg.Redis.Instances["cache-eu"] = instance

connector, err := dcpredis.NewConnectorBuilder(&cfg).Build()
```

### Dcp Configuration

Check out on [go-dcp](https://github.com/Trendyol/go-dcp#configuration)

### Redis Specific Configuration

| Variable                     | Type                   | Required | Default | Description                                                                                        |
| ---------------------------- | ---------------------- | -------- | ------- | -------------------------------------------------------------------------------------------------- |
| `redis.host`                 | string                 | yes\*    |         | Redis host (\*required if not using Sentinel)                                                      |
| `redis.port`                 | int                    | no       | 6379    | Redis port                                                                                         |
| `redis.username`             | string                 | no       |         | Redis username (Redis 6.0+)                                                                        |
| `redis.password`             | string                 | no       |         | Redis password                                                                                     |
| `redis.db`                   | int                    | no       | 0       | Redis database number                                                                              |
| `redis.defaultTTL`           | time.Duration          | no       |         | Default TTL for keys                                                                               |
| `redis.batchTickerDuration`  | time.Duration          | no       | 10s     | Batch is being flushed automatically at specific time intervals for long waiting messages in batch |
| `redis.collectionKeyMapping` | []CollectionKeyMapping | no       |         | Will be used for default mapper. Please read the next topic.                                       |
| `redis.sentinel`             | RedisSentinel          | no       |         | Redis Sentinel configuration for high availability                                                 |
| `redis.cluster`              | RedisCluster           | no       |         | Redis Cluster configuration for horizontal scaling                                                 |
| `redis.instances`            | map[string]RedisInstance | no     |         | Additional named Redis instances. Please read the Multiple Redis Instances topic.                  |

### Redis Instance Configuration

Every entry under `redis.instances.<name>` accepts the same connection fields as the top-level `redis` block.

| Variable                           | Type          | Required | Default | Description                                            |
| ---------------------------------- | ------------- | -------- | ------- | ------------------------------------------------------ |
| `redis.instances.<name>.host`      | string        | yes\*    |         | Redis host (\*required if not using Sentinel/Cluster)  |
| `redis.instances.<name>.port`      | int           | no       | 6379    | Redis port                                             |
| `redis.instances.<name>.username`  | string        | no       |         | Redis username (Redis 6.0+)                            |
| `redis.instances.<name>.password`  | string        | no       |         | Redis password                                         |
| `redis.instances.<name>.db`        | int           | no       | 0       | Redis database number                                  |
| `redis.instances.<name>.sentinel`  | RedisSentinel | no       |         | Sentinel configuration, same fields as `redis.sentinel` |
| `redis.instances.<name>.cluster`   | RedisCluster  | no       |         | Cluster configuration, same fields as `redis.cluster`   |

### Redis Sentinel Configuration

| Variable                       | Type     | Required | Default | Description                                                       |
| ------------------------------ | -------- | -------- | ------- | ----------------------------------------------------------------- |
| `redis.sentinel.masterName`    | string   | yes      |         | Name of the Redis master in Sentinel configuration                |
| `redis.sentinel.sentinelAddrs` | []string | yes      |         | List of Sentinel addresses (host:port)                            |
| `redis.sentinel.username`      | string   | no       |         | Sentinel-specific username (overrides redis.username if provided) |
| `redis.sentinel.password`      | string   | no       |         | Sentinel-specific password (overrides redis.password if provided) |

### Redis Cluster Configuration

| Variable                     | Type     | Required | Default | Description                                                         |
| ---------------------------- | -------- | -------- | ------- | ------------------------------------------------------------------- |
| `redis.cluster.addrs`        | []string | yes      |         | List of Redis Cluster node addresses (host:port)                    |
| `redis.cluster.username`     | string   | no       |         | Cluster-specific username (overrides redis.username if provided)    |
| `redis.cluster.password`     | string   | no       |         | Cluster-specific password (overrides redis.password if provided)    |
| `redis.cluster.routeByLatency` | bool   | no       | false   | Route commands to the node with lowest latency                      |
| `redis.cluster.routeRandomly`  | bool   | no       | false   | Route commands randomly to distribute load                          |
| `redis.cluster.readOnly`       | bool   | no       | false   | Allow read operations on replica nodes                              |

### Collection Key Mapping Configuration

Collection key mapping configuration is optional. The default mapper needs an entry for every collection it handles. With a custom mapper function only `target` is used, so you need entries only for collections you want routed to another Redis instance; the remaining fields are ignored.

| Variable                                   | Type          | Required | Default | Description                                                      |
| ------------------------------------------ | ------------- | -------- | ------- | ---------------------------------------------------------------- |
| `redis.collectionKeyMapping[].collection`  | string        | yes      |         | Couchbase collection name                                        |
| `redis.collectionKeyMapping[].keyPrefix`   | string        | no       |         | Prefix to add to Redis keys                                      |
| `redis.collectionKeyMapping[].keySuffix`   | string        | no       |         | Suffix to add to Redis keys                                      |
| `redis.collectionKeyMapping[].storageType` | string        | no       | string  | Storage type: "string", "hash", "json"                           |
| `redis.collectionKeyMapping[].ttl`         | time.Duration | no       |         | TTL for this collection's keys                                   |
| `redis.collectionKeyMapping[].hashField`   | string        | no       | value   | Field name when using hash storage type                          |
| `redis.collectionKeyMapping[].target`      | string        | no       |         | Name of a `redis.instances` entry; empty writes to the default Redis |

## Exposed metrics

| Metric Name                                     | Description                   | Labels | Value Type |
| ----------------------------------------------- | ----------------------------- | ------ | ---------- |
| redis_connector_latency_ms                      | Time to adding to the batch.  | N/A    | Gauge      |
| redis_connector_bulk_request_process_latency_ms | Time to process bulk request. | N/A    | Gauge      |

You can also use all DCP-related metrics explained [here](https://github.com/Trendyol/go-dcp#exposed-metrics).
All DCP-related metrics are automatically injected. It means you don't need to do anything.

## Contributing

Go Dcp Redis is always open for direct contributions. For more information please check
our [Contribution Guideline document](./CONTRIBUTING.md).

## License

Released under the [MIT License](LICENSE).
