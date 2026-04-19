package policy

import (
	"fmt"

	"github.com/dmitryBe/weaver/internal/dsl/resilient"
)

type Option struct {
	Name  string
	Value any
}

// Resilience declares pipeline-level resilience defaults. The config is
// applied independently to each eligible I/O node rather than as a shared
// pipeline-wide budget.
func Resilience(cfg any) Option {
	return Option{
		Name:  "resilience",
		Value: cfg,
	}
}

func ResolveResilience(options []Option) (*resilient.Config, error) {
	var resolved *resilient.Config
	for _, option := range options {
		if option.Name != "resilience" {
			continue
		}

		var cfg resilient.Config
		switch current := option.Value.(type) {
		case resilient.Config:
			cfg = current
		case *resilient.Config:
			if current == nil {
				return nil, fmt.Errorf("resilience config must not be nil")
			}
			cfg = *current
		default:
			return nil, fmt.Errorf("resilience option must be resilient.Config, got %T", option.Value)
		}

		if err := resilient.Validate(cfg); err != nil {
			return nil, fmt.Errorf("invalid resilience config: %w", err)
		}

		copied := cfg
		resolved = &copied
	}

	return resolved, nil
}
