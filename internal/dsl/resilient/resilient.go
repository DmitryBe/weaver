package resilient

import (
	"fmt"
	"time"
)

const (
	defaultMaxLatency = 5 * time.Second
	defaultMaxRetries = 1
)

// Config defines pipeline-level resilience defaults that are enforced
// independently on each eligible external-I/O node. It is not a shared
// pipeline-wide budget.
type Config struct {
	MaxLatency time.Duration
	MaxRetries int
}

// Default returns an explicit resilience config for pipeline-level declaration.
// The resulting settings are applied independently to each eligible I/O node.
func Default(maxLatency time.Duration, maxRetries int) Config {
	return Config{
		MaxLatency: maxLatency,
		MaxRetries: maxRetries,
	}
}

func Standard() Config {
	return Config{
		MaxLatency: defaultMaxLatency,
		MaxRetries: defaultMaxRetries,
	}
}

func Validate(cfg Config) error {
	if cfg.MaxLatency <= 0 {
		return fmt.Errorf("max latency must be > 0")
	}
	if cfg.MaxRetries < 0 {
		return fmt.Errorf("max retries must be >= 0")
	}
	return nil
}
