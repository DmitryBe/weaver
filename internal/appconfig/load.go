package appconfig

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	// galileo "github.com/careem/galileo-go"
	"github.com/ilyakaznacheev/cleanenv"
	"gopkg.in/yaml.v3"
)

const (
	configFileEnv   = "CONFIG_FILE"
	galileoProject  = "xfy"
	galileoVariable = "xfy/orchestrator_config"
)

// Load reads the configuration. CONFIG_FILE env selects local file loading;
// otherwise production Galileo loading is used.
func Load() (*AppConfig, error) {
	if f := strings.TrimSpace(os.Getenv(configFileEnv)); f != "" {
		return LoadFromPath(f)
	}
	cfg, err := loadFromGalileo()
	if err != nil {
		return nil, fmt.Errorf("galileo config: %w", err)
	}
	if err := Validate(cfg); err != nil {
		return nil, fmt.Errorf("galileo config: %w", err)
	}
	return cfg, nil
}

func LoadFromPath(path string) (*AppConfig, error) {
	cfg, err := loadFromFile(path)
	if err != nil {
		return nil, fmt.Errorf("config file %q: %w", path, err)
	}
	if err := Validate(cfg); err != nil {
		return nil, fmt.Errorf("config file %q: %w", path, err)
	}
	return cfg, nil
}

// loadFromFile reads a YAML or JSON config file, expands ${ENV_VAR} references,
// then overlays fields that carry env tags via cleanenv.
func loadFromFile(path string) (*AppConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	expanded := os.ExpandEnv(string(data))
	cfg := &AppConfig{}

	switch strings.ToLower(filepath.Ext(path)) {
	case ".yaml", ".yml":
		if err := yaml.Unmarshal([]byte(expanded), cfg); err != nil {
			return nil, err
		}
	case ".json":
		if err := json.Unmarshal([]byte(expanded), cfg); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported format %q (use .yaml, .yml or .json)", filepath.Ext(path))
	}

	if err := cleanenv.ReadEnv(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

// loadFromGalileo fetches the orchestrator config JSON from Galileo, expands
// ${ENV_VAR} references, then overlays fields that carry env tags.
func loadFromGalileo() (*AppConfig, error) {
	// sdk := galileo.New(galileoProject, galileo.WithoutTracking())
	// raw := sdk.GetString(galileoVariable, nil, "{}")
	// raw = os.ExpandEnv(raw)
	// cfg := &AppConfig{}

	// if err := json.Unmarshal([]byte(raw), cfg); err != nil {
	// 	return nil, fmt.Errorf("unmarshal: %w", err)
	// }

	// if err := cleanenv.ReadEnv(cfg); err != nil {
	// 	return nil, err
	// }
	// return cfg, nil
	return nil, fmt.Errorf("not implemented")
}
