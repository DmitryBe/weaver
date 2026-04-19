package appconfig

import (
	"fmt"
	"strings"

	"github.com/dmitryBe/weaver/internal/metrics"
)

func Validate(cfg *AppConfig) error {
	if cfg == nil {
		return fmt.Errorf("app config is nil")
	}
	if err := cfg.Normalize(); err != nil {
		return err
	}
	if cfg.Port <= 0 {
		return fmt.Errorf("port must be greater than zero")
	}
	if strings.TrimSpace(cfg.Env) == "" {
		return fmt.Errorf("env must not be empty")
	}
	if err := validateFeatureStore(cfg.FeatureStore); err != nil {
		return err
	}
	if err := validateCache(cfg.Cache); err != nil {
		return err
	}
	normalizedMetrics, err := metrics.NormalizeConfig(cfg.Metrics)
	if err != nil {
		return err
	}
	cfg.Metrics = normalizedMetrics
	for name, kg := range cfg.KnowledgeGraphs {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("knowledge graph name must not be empty")
		}
		if err := validateKnowledgeGraph(name, kg); err != nil {
			return err
		}
	}
	return nil
}

func validateFeatureStore(cfg FeatureStoreConfig) error {
	switch cfg.Type {
	case "file":
		if strings.TrimSpace(cfg.Dir) == "" {
			return fmt.Errorf("feature_store.dir must not be empty when feature_store.type=file")
		}
		return nil
	case "hyper":
		return nil
	case "":
		return fmt.Errorf("feature_store.type must not be empty")
	default:
		return fmt.Errorf("unsupported feature_store.type %q", cfg.Type)
	}
}

func validateCache(cfg CacheConfig) error {
	switch cfg.Type {
	case "":
		return nil
	case "inmemory_lru":
		if cfg.DefaultTTL <= 0 {
			return fmt.Errorf("cache.default_ttl must be greater than zero when cache.type=inmemory_lru")
		}
		if cfg.InMemoryLRU.Capacity <= 0 {
			return fmt.Errorf("cache.inmemory_lru.capacity must be greater than zero when cache.type=inmemory_lru")
		}
		return nil
	default:
		return fmt.Errorf("unsupported cache.type %q", cfg.Type)
	}
}

func validateKnowledgeGraph(name string, cfg KnowledgeGraphConfig) error {
	switch cfg.Type {
	case "bolt":
		if strings.TrimSpace(cfg.URI) == "" {
			return fmt.Errorf("knowledge graph %q uri must not be empty", name)
		}
		if strings.TrimSpace(cfg.User) == "" {
			return fmt.Errorf("knowledge graph %q user must not be empty", name)
		}
		if cfg.Password == "" {
			return fmt.Errorf("knowledge graph %q password must not be empty", name)
		}
		return nil
	case "":
		return fmt.Errorf("knowledge graph %q type must not be empty", name)
	default:
		return fmt.Errorf("unsupported knowledge graph %q type %q", name, cfg.Type)
	}
}
