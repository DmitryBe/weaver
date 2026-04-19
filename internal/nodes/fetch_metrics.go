package nodes

import (
	"context"
	"fmt"

	"github.com/dmitryBe/weaver/internal/dsl/op"
	"github.com/dmitryBe/weaver/internal/metrics"
	"github.com/dmitryBe/weaver/internal/runtime"
)

func resolveFetchValue(
	ctx context.Context,
	env *runtime.ExecEnv,
	pipelineName string,
	stageName string,
	scope string,
	entity string,
	current *op.FetchOp,
	row map[string]any,
) (any, error) {
	value, ok := row[current.Source]
	switch {
	case !ok:
		if current.DefaultValue == nil {
			return nil, runtime.NewExecutionError(
				runtime.CategoryDataUnavailable,
				"required feature data unavailable for requested key",
				fmt.Errorf("missing feature %q", current.Source),
			)
		}
		recordFeatureDefault(ctx, env, pipelineName, stageName, scope, entity, current.Source, metrics.ReasonMissing)
		return current.DefaultValue, nil
	case value == nil:
		if current.DefaultValue == nil {
			return nil, runtime.NewExecutionError(
				runtime.CategoryDataUnavailable,
				"required feature data unavailable for requested key",
				fmt.Errorf("null feature %q", current.Source),
			)
		}
		recordFeatureDefault(ctx, env, pipelineName, stageName, scope, entity, current.Source, metrics.ReasonNull)
		return current.DefaultValue, nil
	default:
		return value, nil
	}
}

func recordFeatureDefault(
	ctx context.Context,
	env *runtime.ExecEnv,
	pipelineName string,
	stageName string,
	scope string,
	entity string,
	feature string,
	reason string,
) {
	env.MetricRecorder().RecordFeatureDefault(ctx, metrics.FeatureDefaultMetric{
		Pipeline:  pipelineName,
		StageName: stageName,
		Scope:     scope,
		Entity:    entity,
		Feature:   feature,
		Reason:    reason,
	})
}
