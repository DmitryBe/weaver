package appconfig

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadUsesConfigFileAndNormalizesNeo4jAlias(t *testing.T) {
	t.Setenv(configFileEnv, filepath.Join(t.TempDir(), "dev.yaml"))
	t.Setenv("NEO4J_DEFAULT_PASSWORD", "super-secret")

	path := os.Getenv(configFileEnv)
	if err := os.WriteFile(path, []byte(`
port: 8080
env: development
feature_store:
  type: file
  dir: testdata/featurestore/unit
neo4j:
  default:
    type: bolt
    uri: "neo4j+s://example.databases.neo4j.io"
    user: "neo4j"
    password: "${NEO4J_DEFAULT_PASSWORD}"
`), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if got, want := cfg.KnowledgeGraphs["default"].Password, "super-secret"; got != want {
		t.Fatalf("unexpected password: got %q want %q", got, want)
	}
}

func TestLoadFromFileSupportsJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dev.json")
	if err := os.WriteFile(path, []byte(`{
  "port": 8080,
  "env": "development",
  "feature_store": {
    "type": "file",
    "dir": "testdata/featurestore/unit"
  },
  "cache": {
    "type": "inmemory_lru",
    "default_ttl": 300000000000,
    "inmemory_lru": {
      "capacity": 128
    }
  },
  "knowledge_graphs": {
    "default": {
      "type": "bolt",
      "uri": "neo4j+s://example.databases.neo4j.io",
      "user": "neo4j",
      "password": "secret"
    }
  }
}`), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	cfg, err := loadFromFile(path)
	if err != nil {
		t.Fatalf("load json config: %v", err)
	}
	if err := Validate(cfg); err != nil {
		t.Fatalf("validate json config: %v", err)
	}

	if got, want := cfg.FeatureStore.Dir, "testdata/featurestore/unit"; got != want {
		t.Fatalf("unexpected feature store dir: got %q want %q", got, want)
	}
	if got, want := cfg.Cache.InMemoryLRU.Capacity, 128; got != want {
		t.Fatalf("unexpected cache capacity: got %d want %d", got, want)
	}
}

func TestLoadFromFileRejectsUnsupportedFormat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dev.toml")
	if err := os.WriteFile(path, []byte(`port = 8080`), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	_, err := loadFromFile(path)
	if err == nil || !strings.Contains(err.Error(), "unsupported format") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadReturnsValidationErrorForInvalidFileConfig(t *testing.T) {
	t.Setenv(configFileEnv, filepath.Join(t.TempDir(), "invalid.yaml"))

	path := os.Getenv(configFileEnv)
	if err := os.WriteFile(path, []byte(`
env: development
feature_store:
  type: file
  dir: testdata/featurestore/unit
`), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "port must be greater than zero") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadFallsBackToGalileoWhenConfigFileIsUnset(t *testing.T) {
	t.Setenv(configFileEnv, "")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "galileo config: not implemented") {
		t.Fatalf("unexpected error: %v", err)
	}
}
