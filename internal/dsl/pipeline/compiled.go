package pipeline

import (
	"github.com/dmitryBe/weaver/internal/dsl/merge"
	"github.com/dmitryBe/weaver/internal/dsl/op"
	"github.com/dmitryBe/weaver/internal/dsl/resilient"
	"github.com/dmitryBe/weaver/internal/dsl/retrieve"
)

type NodeKind string

const (
	NodeKindContextFetch     NodeKind = "context_fetch"
	NodeKindContextModel     NodeKind = "context_model"
	NodeKindContextMutate    NodeKind = "context_mutate"
	NodeKindRetrieveSource   NodeKind = "retrieve_source"
	NodeKindRetrieveMerge    NodeKind = "retrieve_merge"
	NodeKindCandidatesFetch  NodeKind = "candidates_fetch"
	NodeKindCandidatesModel  NodeKind = "candidates_model"
	NodeKindCandidatesMutate NodeKind = "candidates_mutate"
)

type CompiledNode struct {
	ID             string
	StageName      string
	Kind           NodeKind
	DependsOn      []string
	Resilience     *resilient.Config
	Ops            []op.Op
	RetrieveSource retrieve.Source
	MergeSpec      *merge.Spec
}

type ExecutionGroup struct {
	Index   int
	NodeIDs []string
}

type CompiledPlan struct {
	SpecName  string
	Nodes     []CompiledNode
	Groups    []ExecutionGroup
	FinalNode string
}
