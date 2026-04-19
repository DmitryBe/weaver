package nodes

import (
	"context"

	"github.com/dmitryBe/weaver/internal/dsl/pipeline"
	"github.com/dmitryBe/weaver/internal/nodeutil"
	"github.com/dmitryBe/weaver/internal/runtime"
)

type RetrieveMergeExecutor struct{}

func (RetrieveMergeExecutor) Execute(ctx context.Context, node pipeline.CompiledNode, in runtime.NodeInput, env *runtime.ExecEnv) (runtime.NodeOutput, error) {
	_ = ctx
	_ = env

	total := 0
	for _, dep := range node.DependsOn {
		total += len(in.Upstreams[dep].Candidates)
	}
	merged := make([]runtime.Candidate, 0, total)
	for _, dep := range node.DependsOn {
		merged = append(merged, in.Upstreams[dep].Candidates...)
	}

	dedupField := ""
	sortField := ""
	desc := false
	if node.MergeSpec != nil {
		dedupField = node.MergeSpec.DedupField
		sortField = node.MergeSpec.SortScoreField
		desc = node.MergeSpec.SortDescending
	}

	merged = nodeutil.DedupCandidatesByField(merged, dedupField)
	if sortField != "" {
		nodeutil.SortCandidatesByField(merged, sortField, desc)
	}
	return runtime.NodeOutput{Candidates: merged}, nil
}
