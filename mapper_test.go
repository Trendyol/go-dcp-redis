package dcpredis

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Trendyol/go-dcp-redis/config"
	"github.com/Trendyol/go-dcp-redis/couchbase"
)

func TestSetCollectionKeyMappings(t *testing.T) {
	mappings := []config.CollectionKeyMapping{
		{
			Collection: "test", KeyPrefix: "testPrefix", KeySuffix: "testSuffix",
			StorageType: "string", TTL: 10 * time.Second,
		},
		{
			Collection: "test2", KeyPrefix: "testPrefix2", KeySuffix: "testSuffix2",
			StorageType: "hash", TTL: 20 * time.Second, HashField: "testHashField",
		},
	}

	SetCollectionKeyMappings(&mappings)

	tests := []struct {
		name   string
		lookup string
		want   config.CollectionKeyMapping
	}{
		{
			name:   "string storage type mapping",
			lookup: "test",
			want: config.CollectionKeyMapping{
				Collection:  "test",
				KeyPrefix:   "testPrefix",
				KeySuffix:   "testSuffix",
				StorageType: "string",
				TTL:         10 * time.Second,
			},
		},
		{
			name:   "hash storage type mapping",
			lookup: "test2",
			want: config.CollectionKeyMapping{
				Collection:  "test2",
				KeyPrefix:   "testPrefix2",
				KeySuffix:   "testSuffix2",
				StorageType: "hash",
				TTL:         20 * time.Second,
				HashField:   "testHashField",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := findCollectionKeyMapping(tt.lookup)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("findCollectionKeyMapping(%q) = %+v, want %+v", tt.lookup, got, tt.want)
			}
		})
	}
}

func TestFindCollectionKeyMappingNotFound(t *testing.T) {
	mappings := []config.CollectionKeyMapping{
		{
			Collection: "test", KeyPrefix: "testPrefix", KeySuffix: "testSuffix",
			StorageType: "string", TTL: 10 * time.Second,
		},
	}

	SetCollectionKeyMappings(&mappings)

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic, got none")
		}
	}()

	findCollectionKeyMapping("unknown")
}

func TestFindCollectionKeyMappingConcurrent(t *testing.T) {
	mappings := []config.CollectionKeyMapping{
		{
			Collection: "col1", KeyPrefix: "p1", KeySuffix: "s1",
			StorageType: "string", TTL: 10 * time.Second,
		},
		{
			Collection: "col2", KeyPrefix: "p2", KeySuffix: "s2",
			StorageType: "string", TTL: 10 * time.Second,
		},
		{
			Collection: "col3", KeyPrefix: "p3", KeySuffix: "s3",
			StorageType: "string", TTL: 10 * time.Second,
		},
	}
	SetCollectionKeyMappings(&mappings)

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)

	collections := []string{"col1", "col2", "col3"}

	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			name := collections[idx%len(collections)]
			got := findCollectionKeyMapping(name)
			if got.Collection != name {
				t.Errorf("Collection = %s, want %s", got.Collection, name)
			}
		}(i)
	}

	wg.Wait()
}

func TestFindCollectionKeyMappingConcurrentWithSet(t *testing.T) {
	mappings := []config.CollectionKeyMapping{
		{
			Collection: "col1", KeyPrefix: "p1", KeySuffix: "s1",
			StorageType: "string", TTL: 10 * time.Second,
		},
		{
			Collection: "col2", KeyPrefix: "p2", KeySuffix: "s2",
			StorageType: "string", TTL: 10 * time.Second,
		},
		{
			Collection: "col3", KeyPrefix: "p3", KeySuffix: "s3",
			StorageType: "string", TTL: 10 * time.Second,
		},
	}
	SetCollectionKeyMappings(&mappings)

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			if idx%10 == 0 {
				updated := []config.CollectionKeyMapping{
					{Collection: "col1", KeyPrefix: "updated1"},
					{Collection: "col2", KeyPrefix: "updated2"},
				}
				SetCollectionKeyMappings(&updated)
			} else {
				got := findCollectionKeyMapping("col1")
				if got.Collection != "col1" {
					t.Errorf("Collection = %s, want col1", got.Collection)
				}
			}
		}(i)
	}

	wg.Wait()
}

func TestDefaultMapperMutation(t *testing.T) {
	setupDefaultMapperMappings()

	tests := []struct {
		name       string
		collection string
	}{
		{"string storage type", "test"},
		{"hash storage type", "test2"},
		{"hash storage type without field", "test4"},
		{"json storage type", "test3"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := couchbase.Context{
				Event: couchbase.Event{
					CollectionName: tt.collection,
					Key:            []byte("testKey"),
					Value:          []byte("testValue"),
					IsMutated:      true,
				},
			}
			got := DefaultMapper(ctx)
			if len(got) != 1 {
				t.Errorf("expected 1 command, got %d", len(got))
			}
		})
	}
}

func TestDefaultMapperDeletion(t *testing.T) {
	setupDefaultMapperMappings()

	tests := []struct {
		name       string
		collection string
	}{
		{"string storage type", "test"},
		{"hash storage type", "test2"},
		{"hash storage type without field", "test4"},
		{"json storage type", "test3"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := couchbase.Context{
				Event: couchbase.Event{
					CollectionName: tt.collection,
					Key:            []byte("testKey"),
					IsDeleted:      true,
				},
			}
			got := DefaultMapper(ctx)
			if len(got) != 1 {
				t.Errorf("expected 1 command, got %d", len(got))
			}
		})
	}
}

func TestDefaultMapperReturnsNil(t *testing.T) {
	setupDefaultMapperMappings()

	ctx := couchbase.Context{
		Event: couchbase.Event{
			CollectionName: "test",
			Key:            []byte("testKey"),
		},
	}
	got := DefaultMapper(ctx)
	if got != nil {
		t.Errorf("expected nil, got %v", got)
	}
}

func setupDefaultMapperMappings() {
	mappings := []config.CollectionKeyMapping{
		{
			Collection: "test", KeyPrefix: "testPrefix",
			KeySuffix: "testSuffix", StorageType: "string", TTL: 10 * time.Second,
		},
		{
			Collection: "test2", KeyPrefix: "testPrefix2",
			KeySuffix: "testSuffix2", StorageType: "hash",
			TTL: 10 * time.Second, HashField: "testHashField",
		},
		{
			Collection: "test3", KeyPrefix: "testPrefix3",
			KeySuffix: "testSuffix3", StorageType: "json", TTL: 10 * time.Second,
		},
		{
			Collection: "test4", KeyPrefix: "testPrefix4",
			KeySuffix: "testSuffix4", StorageType: "hash", TTL: 10 * time.Second,
		},
	}
	SetCollectionKeyMappings(&mappings)
}
