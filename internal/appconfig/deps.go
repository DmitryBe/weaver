package appconfig

import (
	"context"
	"fmt"
	"log"

	cacheadapter "github.com/dmitryBe/weaver/internal/adapters/cache"
	"github.com/dmitryBe/weaver/internal/adapters/featurestore"
	neoadapter "github.com/dmitryBe/weaver/internal/adapters/neo4j"
	"github.com/dmitryBe/weaver/internal/metrics"
	"github.com/dmitryBe/weaver/internal/runtime"
)

func BuildExecEnv(ctx context.Context, cfg *AppConfig) (*runtime.ExecEnv, func(), error) {
	metricsBundle, err := metrics.New(ctx, cfg.Metrics)
	if err != nil {
		return nil, nil, fmt.Errorf("build metrics: %w", err)
	}

	env := &runtime.ExecEnv{
		FeatureStoreFetchMaxParallelism: cfg.FeatureStoreFetchMaxParallelism,
		KnowledgeGraphs:                 make(map[string]runtime.KnowledgeGraph, len(cfg.KnowledgeGraphs)),
		Metrics:                         metricsBundle.Recorder,
		MetricsHandler:                  metricsBundle.Handler,
		MetricsPath:                     metricsBundle.HandlerPath,
	}

	switch cfg.FeatureStore.Type {
	case "file":
		env.FeatureStore = featurestore.NewFileStore(cfg.FeatureStore.Dir)
	case "hyper":
		return nil, nil, fmt.Errorf("unsupported feature store type %q", cfg.FeatureStore.Type)
	default:
		return nil, nil, fmt.Errorf("unsupported feature store type %q", cfg.FeatureStore.Type)
	}

	switch cfg.Cache.Type {
	case "":
	case "inmemory_lru":
		env.Cache = cacheadapter.NewInMemoryLRU(cfg.Cache.InMemoryLRU.Capacity)
		env.CacheDefaultTTL = cfg.Cache.DefaultTTL
	default:
		return nil, nil, fmt.Errorf("unsupported cache type %q", cfg.Cache.Type)
	}

	cleanup := func() {
		if err := metricsBundle.Shutdown(ctx); err != nil {
			log.Printf("shutdown metrics: %v", err)
		}
	}
	if len(cfg.KnowledgeGraphs) == 0 {
		return env, cleanup, nil
	}

	clients := make([]runtime.KnowledgeGraph, 0, len(cfg.KnowledgeGraphs))
	for name, kgCfg := range cfg.KnowledgeGraphs {
		switch kgCfg.Type {
		case "bolt":
			client, err := neoadapter.NewBoltClient(ctx, neoadapter.ClientConfig{
				URI:      kgCfg.URI,
				Username: kgCfg.User,
				Password: kgCfg.Password,
				Database: kgCfg.Database,
			})
			if err != nil {
				for _, current := range clients {
					_ = current.Close(ctx)
				}
				return nil, nil, err
			}
			env.KnowledgeGraphs[name] = client
			clients = append(clients, client)
		default:
			for _, current := range clients {
				_ = current.Close(ctx)
			}
			return nil, nil, fmt.Errorf("unsupported knowledge graph type %q", kgCfg.Type)
		}
	}

	cleanup = func() {
		for _, client := range clients {
			if err := client.Close(ctx); err != nil {
				log.Printf("close knowledge graph client: %v", err)
			}
		}
		if err := metricsBundle.Shutdown(ctx); err != nil {
			log.Printf("shutdown metrics: %v", err)
		}
	}
	return env, cleanup, nil
}
