package cache

import (
	"context"
	"testing"
	"time"

	"github.com/dmitryBe/weaver/internal/runtime"
)

func TestInMemoryLRUGetSet(t *testing.T) {
	store := NewInMemoryLRU(2)

	err := store.Set(context.Background(), "suite.alpha:key", runtime.State{
		Context: runtime.Context{"city": "almaty"},
	}, time.Minute)
	if err != nil {
		t.Fatalf("set cache entry: %v", err)
	}

	got, ok, err := store.Get(context.Background(), "suite.alpha:key")
	if err != nil {
		t.Fatalf("get cache entry: %v", err)
	}
	if !ok {
		t.Fatal("expected cache hit")
	}
	if got.Context["city"] != "almaty" {
		t.Fatalf("unexpected cached context: %#v", got.Context)
	}
}

func TestInMemoryLRUExpiresEntries(t *testing.T) {
	store := NewInMemoryLRU(2)

	if err := store.Set(context.Background(), "suite.alpha:key", runtime.State{}, 10*time.Millisecond); err != nil {
		t.Fatalf("set cache entry: %v", err)
	}

	time.Sleep(25 * time.Millisecond)

	_, ok, err := store.Get(context.Background(), "suite.alpha:key")
	if err != nil {
		t.Fatalf("get cache entry: %v", err)
	}
	if ok {
		t.Fatal("expected cache miss after ttl")
	}
}

func TestInMemoryLRUEvictsLeastRecentlyUsed(t *testing.T) {
	store := NewInMemoryLRU(1)

	if err := store.Set(context.Background(), "first", runtime.State{}, time.Minute); err != nil {
		t.Fatalf("set first entry: %v", err)
	}
	if err := store.Set(context.Background(), "second", runtime.State{}, time.Minute); err != nil {
		t.Fatalf("set second entry: %v", err)
	}

	if _, ok, err := store.Get(context.Background(), "first"); err != nil {
		t.Fatalf("get first entry: %v", err)
	} else if ok {
		t.Fatal("expected first entry to be evicted")
	}
}

func TestInMemoryLRUZerosOrNegativeCapacityDefaultsToOne(t *testing.T) {
	store := NewInMemoryLRU(0)

	if err := store.Set(context.Background(), "first", runtime.State{}, time.Minute); err != nil {
		t.Fatalf("set first entry: %v", err)
	}
	if err := store.Set(context.Background(), "second", runtime.State{}, time.Minute); err != nil {
		t.Fatalf("set second entry: %v", err)
	}

	if _, ok, err := store.Get(context.Background(), "first"); err != nil {
		t.Fatalf("get first entry: %v", err)
	} else if ok {
		t.Fatal("expected first entry to be evicted")
	}
}
