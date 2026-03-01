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
		{Collection: "test", KeyPrefix: "testPrefix", KeySuffix: "testSuffix", StorageType: "string", TTL: 10 * time.Second},
		{Collection: "test2", KeyPrefix: "testPrefix2", KeySuffix: "testSuffix2", StorageType: "hash", TTL: 20 * time.Second, HashField: "testHashField"},
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
		{Collection: "test", KeyPrefix: "testPrefix", KeySuffix: "testSuffix", StorageType: "string", TTL: 10 * time.Second},
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
		{Collection: "col1", KeyPrefix: "p1", KeySuffix: "s1", StorageType: "string", TTL: 10 * time.Second},
		{Collection: "col2", KeyPrefix: "p2", KeySuffix: "s2", StorageType: "string", TTL: 10 * time.Second},
		{Collection: "col3", KeyPrefix: "p3", KeySuffix: "s3", StorageType: "string", TTL: 10 * time.Second},
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
		{Collection: "col1", KeyPrefix: "p1", KeySuffix: "s1", StorageType: "string", TTL: 10 * time.Second},
		{Collection: "col2", KeyPrefix: "p2", KeySuffix: "s2", StorageType: "string", TTL: 10 * time.Second},
		{Collection: "col3", KeyPrefix: "p3", KeySuffix: "s3", StorageType: "string", TTL: 10 * time.Second},
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

func TestDefaultMapper(t *testing.T) {
	mappings := []config.CollectionKeyMapping{
		{Collection: "test", KeyPrefix: "testPrefix", KeySuffix: "testSuffix", StorageType: "string", TTL: 10 * time.Second},
		{Collection: "test2", KeyPrefix: "testPrefix2", KeySuffix: "testSuffix2", StorageType: "hash", TTL: 10 * time.Second, HashField: "testHashField"},
		{Collection: "test3", KeyPrefix: "testPrefix3", KeySuffix: "testSuffix3", StorageType: "json", TTL: 10 * time.Second},
		{Collection: "test4", KeyPrefix: "testPrefix4", KeySuffix: "testSuffix4", StorageType: "hash", TTL: 10 * time.Second},
	}
	SetCollectionKeyMappings(&mappings)

	tests := []struct {
		name    string
		ctx     couchbase.Context
		wantLen int
		wantNil bool
	}{
		{
			name: "mutation event returns set command with string storage type",
			ctx: couchbase.Context{
				Event: couchbase.Event{
					CollectionName: "test",
					Key:            []byte("testKey"),
					Value:          []byte("testValue"),
					IsMutated:      true,
				},
			},
			wantLen: 1,
		},
		{
			name: "mutation event returns set command with hash storage type",
			ctx: couchbase.Context{
				Event: couchbase.Event{
					CollectionName: "test2",
					Key:            []byte("testKey"),
					Value:          []byte("testValue"),
					IsMutated:      true,
				},
			},
			wantLen: 1,
		},
		{
			name: "mutation event returns set command with hash storage type without field",
			ctx: couchbase.Context{
				Event: couchbase.Event{
					CollectionName: "test4",
					Key:            []byte("testKey"),
					Value:          []byte("testValue"),
					IsMutated:      true,
				},
			},
			wantLen: 1,
		},
		{
			name: "mutation event returns set command with json storage type",
			ctx: couchbase.Context{
				Event: couchbase.Event{
					CollectionName: "test3",
					Key:            []byte("testKey"),
					Value:          []byte("testValue"),
					IsMutated:      true,
				},
			},
			wantLen: 1,
		},
		{
			name: "deletion event returns delete command with string storage type",
			ctx: couchbase.Context{
				Event: couchbase.Event{
					CollectionName: "test",
					Key:            []byte("testKey"),
					IsDeleted:      true,
				},
			},
			wantLen: 1,
		},
		{
			name: "deletion event returns delete command with hash storage type",
			ctx: couchbase.Context{
				Event: couchbase.Event{
					CollectionName: "test2",
					Key:            []byte("testKey"),
					IsDeleted:      true,
				},
			},
			wantLen: 1,
		},
		{
			name: "deletion event returns delete command with hash storage type without field",
			ctx: couchbase.Context{
				Event: couchbase.Event{
					CollectionName: "test4",
					Key:            []byte("testKey"),
					IsDeleted:      true,
				},
			},
			wantLen: 1,
		},
		{
			name: "deletion event returns delete command with json storage type",
			ctx: couchbase.Context{
				Event: couchbase.Event{
					CollectionName: "test3",
					Key:            []byte("testKey"),
					IsDeleted:      true,
				},
			},
			wantLen: 1,
		},
		{
			name: "no flags set returns nil",
			ctx: couchbase.Context{
				Event: couchbase.Event{
					CollectionName: "test",
					Key:            []byte("testKey"),
				},
			},
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DefaultMapper(tt.ctx)
			if tt.wantNil {
				if got != nil {
					t.Errorf("expected nil, got %v", got)
				}
				return
			}
			if len(got) != tt.wantLen {
				t.Errorf("expected %d command(s), got %d", tt.wantLen, len(got))
			}
		})
	}
}
