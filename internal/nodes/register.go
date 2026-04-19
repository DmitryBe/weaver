package nodes

import (
	"github.com/dmitryBe/weaver/internal/dsl/pipeline"
	"github.com/dmitryBe/weaver/internal/runtime"
)

func RegisterDefaults(registry *runtime.Registry) {
	registry.Register(pipeline.NodeKindContextFetch, ContextFetchExecutor{})
	registry.Register(pipeline.NodeKindContextModel, ContextModelExecutor{})
	registry.Register(pipeline.NodeKindContextMutate, ContextMutateExecutor{})
	registry.Register(pipeline.NodeKindRetrieveSource, RetrieveSourceExecutor{})
	registry.Register(pipeline.NodeKindRetrieveMerge, RetrieveMergeExecutor{})
	registry.Register(pipeline.NodeKindCandidatesFetch, CandidatesFetchExecutor{})
	registry.Register(pipeline.NodeKindCandidatesModel, CandidatesModelExecutor{})
	registry.Register(pipeline.NodeKindCandidatesMutate, CandidatesMutateExecutor{})
}
