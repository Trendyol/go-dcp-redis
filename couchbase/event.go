package couchbase

import (
	"time"

	"github.com/Trendyol/go-dcp/tracing"
	"github.com/redis/go-redis/v9"
)

type Event struct {
	CollectionName string
	EventTime      time.Time
	Key            []byte
	Value          []byte
	Cas            uint64
	VbID           uint16
	IsDeleted      bool
	IsExpired      bool
	IsMutated      bool
}

type EventParams struct {
	EventTime      time.Time
	CollectionName string
	Key            []byte
	Value          []byte
	Cas            uint64
	VbID           uint16
}

func NewDeleteEventContext(listenerTrace tracing.ListenerTrace, redisClient redis.Cmdable, params EventParams) Context {
	return Context{
		Tracer:      listenerTrace,
		RedisClient: redisClient,
		Event: Event{
			Key:            params.Key,
			Value:          params.Value,
			IsDeleted:      true,
			CollectionName: params.CollectionName,
			EventTime:      params.EventTime,
			Cas:            params.Cas,
			VbID:           params.VbID,
		},
	}
}

func NewExpireEventContext(listenerTrace tracing.ListenerTrace, redisClient redis.Cmdable, params EventParams) Context {
	return Context{
		Tracer:      listenerTrace,
		RedisClient: redisClient,
		Event: Event{
			Key:            params.Key,
			Value:          params.Value,
			IsExpired:      true,
			CollectionName: params.CollectionName,
			EventTime:      params.EventTime,
			Cas:            params.Cas,
			VbID:           params.VbID,
		},
	}
}

func NewMutateEventContext(listenerTrace tracing.ListenerTrace, redisClient redis.Cmdable, params EventParams) Context {
	return Context{
		Tracer:      listenerTrace,
		RedisClient: redisClient,
		Event: Event{
			Key:            params.Key,
			Value:          params.Value,
			IsMutated:      true,
			CollectionName: params.CollectionName,
			EventTime:      params.EventTime,
			Cas:            params.Cas,
			VbID:           params.VbID,
		},
	}
}
