package cache

import (
	"context"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/dmitryBe/weaver/internal/runtime"
)

type InMemoryLRU struct {
	cache *lru.Cache[string, inMemoryEntry]
}

type inMemoryEntry struct {
	value     runtime.State
	expiresAt time.Time
}

func NewInMemoryLRU(capacity int) *InMemoryLRU {
	if capacity <= 0 {
		capacity = 1
	}
	cache, err := lru.New[string, inMemoryEntry](capacity)
	if err != nil {
		panic(err)
	}
	return &InMemoryLRU{
		cache: cache,
	}
}

func (c *InMemoryLRU) Get(_ context.Context, key string) (runtime.State, bool, error) {
	entry, ok := c.cache.Get(key)
	if !ok {
		return runtime.State{}, false, nil
	}
	if isExpired(entry.expiresAt) {
		c.cache.Remove(key)
		return runtime.State{}, false, nil
	}
	return runtime.CloneState(entry.value), true, nil
}

func (c *InMemoryLRU) Set(_ context.Context, key string, value runtime.State, ttl time.Duration) error {
	c.cache.Add(key, inMemoryEntry{
		value:     runtime.CloneState(value),
		expiresAt: expiresAtFromTTL(ttl),
	})
	return nil
}

func expiresAtFromTTL(ttl time.Duration) time.Time {
	if ttl <= 0 {
		return time.Time{}
	}
	return time.Now().Add(ttl)
}

func isExpired(expiresAt time.Time) bool {
	return !expiresAt.IsZero() && time.Now().After(expiresAt)
}
