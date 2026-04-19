package nodes

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/dmitryBe/weaver/internal/dsl/op"
	"github.com/dmitryBe/weaver/internal/dsl/pipeline"
	"github.com/dmitryBe/weaver/internal/metrics"
	"github.com/dmitryBe/weaver/internal/nodeutil"
	"github.com/dmitryBe/weaver/internal/runtime"
)

type ContextFetchExecutor struct{}

type contextFetchRequest struct {
	entity   string
	ops      []*op.FetchOp
	features []string
	key      runtime.Key
}

type contextFetchResult struct {
	request contextFetchRequest
	rows    []map[string]any
}

func (ContextFetchExecutor) Execute(ctx context.Context, node pipeline.CompiledNode, in runtime.NodeInput, env *runtime.ExecEnv) (runtime.NodeOutput, error) {
	if err := nodeutil.EnsureFeatureStore(ctx, env); err != nil {
		return runtime.NodeOutput{}, err
	}

	out := runtime.CloneContext(in.State.Context)
	entityOps, err := groupContextFetchOpsByEntity(node)
	if err != nil {
		return runtime.NodeOutput{}, err
	}
	requests, err := buildContextFetchRequests(entityOps, in.State)
	if err != nil {
		return runtime.NodeOutput{}, err
	}
	results, err := fetchContextResults(ctx, env, in.State.Meta.Pipeline, node.StageName, requests)
	if err != nil {
		return runtime.NodeOutput{}, err
	}
	if err := applyContextRows(ctx, env, in.State.Meta.Pipeline, node.StageName, node.ID, results, out); err != nil {
		return runtime.NodeOutput{}, err
	}

	return runtime.NodeOutput{Context: out}, nil
}

func groupContextFetchOpsByEntity(node pipeline.CompiledNode) (map[string][]*op.FetchOp, error) {
	entityOps := make(map[string][]*op.FetchOp)
	for _, raw := range node.Ops {
		current, ok := raw.(*op.FetchOp)
		if !ok {
			return nil, fmt.Errorf("node %s expected FetchOp, got %T", node.ID, raw)
		}
		entity, err := nodeutil.InferFeatureEntity(current.Source)
		if err != nil {
			return nil, err
		}
		entityOps[entity] = append(entityOps[entity], current)
	}
	return entityOps, nil
}

func buildContextFetchRequests(entityOps map[string][]*op.FetchOp, state runtime.State) ([]contextFetchRequest, error) {
	entities := make([]string, 0, len(entityOps))
	for entity := range entityOps {
		entities = append(entities, entity)
	}
	sort.Strings(entities)

	requests := make([]contextFetchRequest, 0, len(entities))
	for _, entity := range entities {
		ops := entityOps[entity]
		features := make([]string, 0, len(ops))
		seenFeatures := make(map[string]struct{}, len(ops))
		keys := make([]runtime.Key, 0, len(ops))
		for _, current := range ops {
			key, err := nodeutil.BuildFeatureKey(current.Source, current.KeyExpr, state, nil)
			if err != nil {
				return nil, err
			}
			if _, ok := seenFeatures[current.Source]; !ok {
				features = append(features, current.Source)
				seenFeatures[current.Source] = struct{}{}
			}
			keys = append(keys, key)
		}
		mergedKey, err := nodeutil.MergeFeatureKeys(keys...)
		if err != nil {
			return nil, err
		}
		requests = append(requests, contextFetchRequest{
			entity:   entity,
			ops:      ops,
			features: features,
			key:      mergedKey,
		})
	}
	return requests, nil
}

func fetchContextResults(ctx context.Context, env *runtime.ExecEnv, pipelineName, stageName string, requests []contextFetchRequest) ([]contextFetchResult, error) {
	return nodeutil.RunParallel(env.FeatureStoreFetchMaxParallelism, requests, func(_ int, request contextFetchRequest) (contextFetchResult, error) {
		startedAt := time.Now()
		rows, err := env.FeatureStore.Fetch(ctx, request.features, request.key)
		env.MetricRecorder().RecordFeatureFetch(ctx, metrics.FeatureFetchMetric{
			Pipeline:  pipelineName,
			StageName: stageName,
			Scope:     metrics.ScopeContext,
			Entity:    request.entity,
			Status:    featureFetchStatus(err),
			Duration:  time.Since(startedAt),
		})
		if err != nil {
			return contextFetchResult{}, err
		}
		if len(rows) != 1 {
			return contextFetchResult{}, runtime.NewExecutionError(
				runtime.CategoryPipelineMisconfigured,
				"pipeline misconfigured: feature store contract mismatch",
				fmt.Errorf("expected 1 feature row for entity %q, got %d", request.entity, len(rows)),
			)
		}
		return contextFetchResult{
			request: request,
			rows:    rows,
		}, nil
	})
}

func applyContextRows(ctx context.Context, env *runtime.ExecEnv, pipelineName, stageName, nodeID string, results []contextFetchResult, out runtime.Context) error {
	for _, result := range results {
		row := result.rows[0]
		for _, current := range result.request.ops {
			value, err := resolveFetchValue(ctx, env, pipelineName, stageName, metrics.ScopeContext, result.request.entity, current, row)
			if err != nil {
				return fmt.Errorf("%w for node %s", err, nodeID)
			}
			castValue, err := nodeutil.CastValue(value, current.FieldType)
			if err != nil {
				return runtime.NewExecutionError(
					runtime.CategoryPipelineMisconfigured,
					"pipeline misconfigured: feature value contract mismatch",
					err,
				)
			}
			out[current.IntoField] = castValue
		}
	}
	return nil
}

func featureFetchStatus(err error) string {
	if err != nil {
		return metrics.StatusError
	}
	return metrics.StatusOK
}
