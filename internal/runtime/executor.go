package runtime

import (
	"context"

	"github.com/dmitryBe/weaver/internal/dsl/pipeline"
)

type NodeExecutor interface {
	Execute(ctx context.Context, node pipeline.CompiledNode, in NodeInput, env *ExecEnv) (NodeOutput, error)
}
