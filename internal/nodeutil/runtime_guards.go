package nodeutil

import (
	"context"
	"fmt"

	"github.com/dmitryBe/weaver/internal/runtime"
)

func EnsureFeatureStore(ctx context.Context, env *runtime.ExecEnv) error {
	if env == nil || env.FeatureStore == nil {
		return fmt.Errorf("feature store is required")
	}
	_ = ctx
	return nil
}

func EnsureKnowledgeGraphs(ctx context.Context, env *runtime.ExecEnv) error {
	if env == nil || len(env.KnowledgeGraphs) == 0 {
		return fmt.Errorf("knowledge graph clients are required")
	}
	_ = ctx
	return nil
}
