package appconfig

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	cacheadapter "github.com/dmitryBe/weaver/internal/adapters/cache"
	"github.com/dmitryBe/weaver/internal/adapters/featurestore"
)

func TestBuildExecEnvBuildsFileFeatureStore(t *testing.T) {
	cfg := &AppConfig{
		FeatureStoreFetchMaxParallelism: 3,
		FeatureStore: FeatureStoreConfig{
			Type: "file",
			Dir:  filepath.Join("..", "..", "testdata", "featurestore", "unit"),
		},
	}

	env, cleanup, err := BuildExecEnv(context.Background(), cfg)
	if err != nil {
		t.Fatalf("BuildExecEnv failed: %v", err)
	}
	defer cleanup()

	if got, want := env.FeatureStoreFetchMaxParallelism, 3; got != want {
		t.Fatalf("unexpected parallelism: got %d want %d", got, want)
	}
	if _, ok := env.FeatureStore.(*featurestore.FileStore); !ok {
		t.Fatalf("expected file feature store, got %T", env.FeatureStore)
	}
	if len(env.KnowledgeGraphs) != 0 {
		t.Fatalf("expected no knowledge graphs, got %d", len(env.KnowledgeGraphs))
	}
}

func TestBuildExecEnvRejectsUnsupportedFeatureStore(t *testing.T) {
	cfg := &AppConfig{
		FeatureStore: FeatureStoreConfig{
			Type: "hyper",
		},
	}

	_, _, err := BuildExecEnv(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected unsupported feature store error")
	}
}

func TestBuildExecEnvRejectsUnsupportedKnowledgeGraph(t *testing.T) {
	cfg := &AppConfig{
		FeatureStore: FeatureStoreConfig{
			Type: "file",
			Dir:  filepath.Join("..", "..", "testdata", "featurestore", "unit"),
		},
		KnowledgeGraphs: map[string]KnowledgeGraphConfig{
			"default": {
				Type: "unsupported",
			},
		},
	}

	_, _, err := BuildExecEnv(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected unsupported knowledge graph error")
	}
}

func TestBuildExecEnvBuildsInMemoryCache(t *testing.T) {
	cfg := &AppConfig{
		FeatureStore: FeatureStoreConfig{
			Type: "file",
			Dir:  filepath.Join("..", "..", "testdata", "featurestore", "unit"),
		},
		Cache: CacheConfig{
			Type:       "inmemory_lru",
			DefaultTTL: time.Minute,
			InMemoryLRU: InMemoryLRUCacheConfig{
				Capacity: 64,
			},
		},
	}

	env, cleanup, err := BuildExecEnv(context.Background(), cfg)
	if err != nil {
		t.Fatalf("BuildExecEnv failed: %v", err)
	}
	defer cleanup()

	if got, want := env.CacheDefaultTTL, time.Minute; got != want {
		t.Fatalf("unexpected cache default ttl: got %s want %s", got, want)
	}
	if _, ok := env.Cache.(*cacheadapter.InMemoryLRU); !ok {
		t.Fatalf("expected in-memory cache, got %T", env.Cache)
	}
}
