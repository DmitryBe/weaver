package nodes

import (
	"context"
	"fmt"

	"github.com/dmitryBe/weaver/internal/dsl/op"
	"github.com/dmitryBe/weaver/internal/dsl/pipeline"
	"github.com/dmitryBe/weaver/internal/nodeutil"
	"github.com/dmitryBe/weaver/internal/runtime"
)

type ContextModelExecutor struct{}

func (ContextModelExecutor) Execute(ctx context.Context, node pipeline.CompiledNode, in runtime.NodeInput, env *runtime.ExecEnv) (runtime.NodeOutput, error) {
	out := runtime.CloneContext(in.State.Context)
	working := in.State
	for _, raw := range node.Ops {
		current, ok := raw.(*op.ModelOp)
		if !ok {
			return runtime.NodeOutput{}, fmt.Errorf("node %s expected ModelOp, got %T", node.ID, raw)
		}
		payload, err := nodeutil.BuildRequest(current.RequestSpec, working, nil)
		if err != nil {
			return runtime.NodeOutput{}, err
		}
		responseValue, err := nodeutil.CallRESTModel(ctx, current.EndpointValue, payload)
		if err != nil {
			return runtime.NodeOutput{}, err
		}
		parsed, err := nodeutil.ParseResponse(current.ResponseSpec, responseValue)
		if err != nil {
			return runtime.NodeOutput{}, err
		}
		out[current.IntoField] = parsed
		working.Context = out
	}

	_ = env
	return runtime.NodeOutput{Context: out}, nil
}
