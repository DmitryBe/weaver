package appconfig

import (
	"fmt"
	"time"

	"github.com/dmitryBe/weaver/internal/metrics"
)

type AppConfig struct {
	Port                            int                             `yaml:"port" json:"port"`
	Env                             string                          `yaml:"env" json:"env"`
	FeatureStoreFetchMaxParallelism int                             `yaml:"feature_store_fetch_max_parallelism" json:"feature_store_fetch_max_parallelism"`
	FeatureStore                    FeatureStoreConfig              `yaml:"feature_store" json:"feature_store"`
	Cache                           CacheConfig                     `yaml:"cache" json:"cache"`
	KnowledgeGraphs                 map[string]KnowledgeGraphConfig `yaml:"knowledge_graphs" json:"knowledge_graphs"`
	Neo4j                           map[string]KnowledgeGraphConfig `yaml:"neo4j" json:"neo4j"`
	Metrics                         metrics.Config                  `yaml:"metrics" json:"metrics"`
}

type FeatureStoreConfig struct {
	Type string `yaml:"type" json:"type"`
	Dir  string `yaml:"dir" json:"dir"`
}

type CacheConfig struct {
	Type        string                 `yaml:"type" json:"type"`
	DefaultTTL  time.Duration          `yaml:"default_ttl" json:"default_ttl"`
	InMemoryLRU InMemoryLRUCacheConfig `yaml:"inmemory_lru" json:"inmemory_lru"`
}

type InMemoryLRUCacheConfig struct {
	Capacity int `yaml:"capacity" json:"capacity"`
}

type KnowledgeGraphConfig struct {
	Type     string `yaml:"type" json:"type"`
	URI      string `yaml:"uri" json:"uri"`
	User     string `yaml:"user" json:"user"`
	Password string `yaml:"password" json:"password"`
	Database string `yaml:"database" json:"database"`
}

func (c *AppConfig) Normalize() error {
	if c == nil {
		return fmt.Errorf("app config is nil")
	}
	if len(c.KnowledgeGraphs) > 0 && len(c.Neo4j) > 0 {
		return fmt.Errorf("config must set only one of knowledge_graphs or neo4j")
	}
	if len(c.KnowledgeGraphs) == 0 && len(c.Neo4j) > 0 {
		c.KnowledgeGraphs = c.Neo4j
	}
	if c.Metrics.Backend == "" {
		if c.Env == "production" || c.Env == "prod" {
			c.Metrics.Backend = metrics.BackendOTLP
		} else {
			c.Metrics.Backend = metrics.BackendNoop
		}
	}
	return nil
}
