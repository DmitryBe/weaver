package nodes

import (
	"context"
	"fmt"

	"github.com/dmitryBe/weaver/internal/dsl/op"
	"github.com/dmitryBe/weaver/internal/dsl/pipeline"
	"github.com/dmitryBe/weaver/internal/nodeutil"
	"github.com/dmitryBe/weaver/internal/runtime"
)

type CandidatesMutateExecutor struct{}

func (CandidatesMutateExecutor) Execute(ctx context.Context, node pipeline.CompiledNode, in runtime.NodeInput, env *runtime.ExecEnv) (runtime.NodeOutput, error) {
	_ = ctx
	_ = env

	out := runtime.CloneCandidates(in.State.Candidates)
	working := in.State
	working.Candidates = out

	for _, raw := range node.Ops {
		switch current := raw.(type) {
		case *op.SetOp:
			for i := range out {
				value, err := nodeutil.EvalExpr(current.Value, working, out[i])
				if err != nil {
					return runtime.NodeOutput{}, err
				}
				value, err = nodeutil.CastValue(value, current.FieldType)
				if err != nil {
					return runtime.NodeOutput{}, err
				}
				out[i][current.Field] = value
			}
		case *op.SortByOp:
			nodeutil.SortCandidatesByField(out, current.Field, current.Dir == op.Desc)
		case *op.TakeOp:
			if current.Limit < len(out) {
				out = out[:current.Limit]
			}
		case *op.DropOp:
			for i := range out {
				for _, field := range current.Fields {
					delete(out[i], field)
				}
			}
		case *op.DropExceptOp:
			for i := range out {
				out[i] = runtime.Candidate(keepOnlyMap(map[string]any(out[i]), current.Fields))
			}
		default:
			return runtime.NodeOutput{}, fmt.Errorf("node %s unsupported candidates mutate op %T", node.ID, raw)
		}
		working.Candidates = out
	}

	return runtime.NodeOutput{Candidates: out}, nil
}

func keepOnlyMap(in map[string]any, fields []string) map[string]any {
	if in == nil {
		return nil
	}
	allowed := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		allowed[field] = struct{}{}
	}
	out := make(map[string]any, len(allowed))
	for field, value := range in {
		if _, ok := allowed[field]; ok {
			out[field] = value
		}
	}
	return out
}
