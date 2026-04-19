package retriever

import (
	"context"
	"fmt"

	"github.com/dmitryBe/weaver/internal/runtime"
)

type KVRequest struct {
	Feature string
	Key     any
	Limit   int
}

func RetrieveKV(ctx context.Context, req KVRequest, store runtime.FeatureStore) ([]runtime.Candidate, error) {
	if store == nil {
		return nil, fmt.Errorf("feature store is required")
	}
	if req.Feature == "" {
		return nil, fmt.Errorf("kv feature is required")
	}

	key, err := toSingleKey(req.Key)
	if err != nil {
		return nil, err
	}

	rows, err := store.Fetch(ctx, []string{req.Feature}, key)
	if err != nil {
		return nil, runtime.NewExecutionError(
			runtime.CategoryPipelineMisconfigured,
			"pipeline misconfigured: required feature definition missing",
			err,
		)
	}
	if len(rows) != 1 {
		return nil, runtime.NewExecutionError(
			runtime.CategoryPipelineMisconfigured,
			"pipeline misconfigured: feature store contract mismatch",
			fmt.Errorf("feature store fetch must return exactly 1 row, got %d", len(rows)),
		)
	}
	row := rows[0]
	value, ok := row[req.Feature]
	if !ok {
		return []runtime.Candidate{}, nil
	}

	candidates, err := decodeCandidatesFromValue(value)
	if err != nil {
		return nil, runtime.NewExecutionError(
			runtime.CategoryPipelineMisconfigured,
			"pipeline misconfigured: feature store contract mismatch",
			err,
		)
	}
	if req.Limit > 0 && len(candidates) > req.Limit {
		candidates = candidates[:req.Limit]
	}
	return candidates, nil
}

func toSingleKey(raw any) (runtime.Key, error) {
	if raw == nil {
		return nil, fmt.Errorf("kv key is required")
	}
	switch current := raw.(type) {
	case map[string]string:
		if len(current) == 0 {
			return nil, fmt.Errorf("kv key must not be empty")
		}
		out := make(runtime.Key, len(current))
		for k, v := range current {
			out[k] = v
		}
		return out, nil
	case map[string]any:
		if len(current) == 0 {
			return nil, fmt.Errorf("kv key must not be empty")
		}
		out := make(runtime.Key, len(current))
		for k, v := range current {
			out[k] = fmt.Sprint(v)
		}
		return out, nil
	default:
		// Common case: expr.Tuple(...) evaluates to a single scalar string (joined by "|").
		return runtime.Key{"key": fmt.Sprint(current)}, nil
	}
}
