package nodes

import (
	"context"
	"fmt"

	"github.com/dmitryBe/weaver/internal/dsl/op"
	"github.com/dmitryBe/weaver/internal/dsl/pipeline"
	"github.com/dmitryBe/weaver/internal/nodeutil"
	"github.com/dmitryBe/weaver/internal/runtime"
)

type ContextMutateExecutor struct{}

func (ContextMutateExecutor) Execute(ctx context.Context, node pipeline.CompiledNode, in runtime.NodeInput, env *runtime.ExecEnv) (runtime.NodeOutput, error) {
	_ = ctx
	_ = env

	out := runtime.CloneContext(in.State.Context)
	working := in.State
	working.Context = out

	for _, raw := range node.Ops {
		switch current := raw.(type) {
		case *op.SetOp:
			value, err := nodeutil.EvalExpr(current.Value, working, nil)
			if err != nil {
				return runtime.NodeOutput{}, err
			}
			value, err = nodeutil.CastValue(value, current.FieldType)
			if err != nil {
				return runtime.NodeOutput{}, err
			}
			out[current.Field] = value
		case *op.DropOp:
			for _, field := range current.Fields {
				delete(out, field)
			}
		case *op.DropExceptOp:
			out = keepOnlyMap(out, current.Fields)
		default:
			return runtime.NodeOutput{}, fmt.Errorf("node %s unsupported context mutate op %T", node.ID, raw)
		}
		working.Context = out
	}

	return runtime.NodeOutput{Context: out}, nil
}
