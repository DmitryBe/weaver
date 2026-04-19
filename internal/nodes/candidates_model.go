package nodes

import (
	"context"
	"fmt"

	"github.com/dmitryBe/weaver/internal/dsl/op"
	"github.com/dmitryBe/weaver/internal/dsl/pipeline"
	"github.com/dmitryBe/weaver/internal/nodeutil"
	"github.com/dmitryBe/weaver/internal/runtime"
)

type CandidatesModelExecutor struct{}

func (CandidatesModelExecutor) Execute(ctx context.Context, node pipeline.CompiledNode, in runtime.NodeInput, env *runtime.ExecEnv) (runtime.NodeOutput, error) {
	out := runtime.CloneCandidates(in.State.Candidates)
	working := runtime.CloneState(in.State)
	working.Candidates = out

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
		scores, err := nodeutil.ToScoreSlice(parsed)
		if err != nil {
			return runtime.NodeOutput{}, err
		}
		if len(scores) != len(out) {
			return runtime.NodeOutput{}, runtime.NewExecutionError(
				runtime.CategoryPipelineMisconfigured,
				"pipeline misconfigured: upstream response contract mismatch",
				fmt.Errorf("node %s expected %d scores, got %d", node.ID, len(out), len(scores)),
			)
		}
		for i := range out {
			out[i][current.IntoField] = scores[i]
		}
		working.Candidates = out
	}

	_ = env
	return runtime.NodeOutput{Candidates: out}, nil
}
