package dcpredis

import (
	"fmt"
	"sync"

	"github.com/Trendyol/go-dcp-redis/config"
	"github.com/Trendyol/go-dcp-redis/couchbase"
	"github.com/Trendyol/go-dcp-redis/redis"
)

type Mapper func(ctx couchbase.Context) []redis.Model

var (
	collectionKeyMappings *[]config.CollectionKeyMapping
	mappingCache          = make(map[string]config.CollectionKeyMapping)
	mappingCacheMu        sync.RWMutex
)

func SetCollectionKeyMappings(mappings *[]config.CollectionKeyMapping) {
	cache := make(map[string]config.CollectionKeyMapping)
	for _, mapping := range *mappings {
		cache[mapping.Collection] = mapping
	}

	mappingCacheMu.Lock()
	defer mappingCacheMu.Unlock()

	collectionKeyMappings = mappings
	mappingCache = cache
}

func DefaultMapper(ctx couchbase.Context) []redis.Model {
	event := ctx.Event
	if event.IsMutated {
		mapping := findCollectionKeyMapping(event.CollectionName)
		command := buildSetCommand(mapping, event)
		return []redis.Model{&command}
	} else if event.IsDeleted || event.IsExpired {
		mapping := findCollectionKeyMapping(event.CollectionName)
		command := buildDeleteCommand(mapping, event)
		return []redis.Model{&command}
	}

	return nil
}

func findCollectionKeyMapping(collectionName string) config.CollectionKeyMapping {
	mappingCacheMu.RLock()
	defer mappingCacheMu.RUnlock()

	if mapping, exists := mappingCache[collectionName]; exists {
		return mapping
	}

	panic(fmt.Sprintf("no mapping found for collection: %s", collectionName))
}

func buildSetCommand(mapping config.CollectionKeyMapping, event couchbase.Event) redis.Raw {
	key := buildRedisKey(mapping, string(event.Key))

	switch mapping.StorageType {
	case "hash":
		field := mapping.HashField
		if field == "" {
			field = "value"
		}
		return redis.Raw{
			Operation: "HSET",
			Key:       key,
			Args:      []any{field, string(event.Value)},
			TTL:       mapping.TTL,
		}
	case "json", "string":
		fallthrough
	default:
		return redis.Raw{
			Operation: "SET",
			Key:       key,
			Value:     string(event.Value),
			TTL:       mapping.TTL,
		}
	}
}

func buildDeleteCommand(mapping config.CollectionKeyMapping, event couchbase.Event) redis.Raw {
	key := buildRedisKey(mapping, string(event.Key))

	switch mapping.StorageType {
	case "hash":
		field := mapping.HashField
		if field == "" {
			field = "value"
		}
		return redis.Raw{
			Operation: "HDEL",
			Key:       key,
			Args:      []any{field},
		}
	default:
		return redis.Raw{
			Operation: "DEL",
			Key:       key,
		}
	}
}

func buildRedisKey(mapping config.CollectionKeyMapping, originalKey string) string {
	return fmt.Sprintf("%s%s%s", mapping.KeyPrefix, originalKey, mapping.KeySuffix)
}
