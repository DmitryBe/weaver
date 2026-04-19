package pipeline

import (
	"fmt"

	"github.com/dmitryBe/weaver/internal/dsl/merge"
	"github.com/dmitryBe/weaver/internal/dsl/op"
	"github.com/dmitryBe/weaver/internal/dsl/policy"
	"github.com/dmitryBe/weaver/internal/dsl/resilient"
	"github.com/dmitryBe/weaver/internal/dsl/retrieve"
)

func Compile(spec *Spec) (*CompiledPlan, error) {
	if err := Validate(spec); err != nil {
		return nil, err
	}

	resilienceCfg, err := policy.ResolveResilience(spec.DefaultOptions)
	if err != nil {
		return nil, ValidationError{
			Path:    "pipeline.defaults",
			Message: err.Error(),
		}
	}

	plan := &CompiledPlan{
		SpecName: spec.Name,
	}

	var previousGroup []string
	for groupIndex, stage := range spec.Stages {
		switch stage.Kind {
		case StageContext, StageCandidates:
			node, err := compileOpStage(stage, previousGroup, resilienceCfg)
			if err != nil {
				return nil, err
			}
			plan.Nodes = append(plan.Nodes, node)
			plan.Groups = append(plan.Groups, ExecutionGroup{
				Index:   len(plan.Groups),
				NodeIDs: []string{node.ID},
			})
			previousGroup = []string{node.ID}

		case StageRetrieve:
			nodes, groups, nextPrevious, err := compileRetrieveStage(stage, previousGroup, len(plan.Groups), resilienceCfg)
			if err != nil {
				return nil, err
			}
			plan.Nodes = append(plan.Nodes, nodes...)
			plan.Groups = append(plan.Groups, groups...)
			previousGroup = nextPrevious

		default:
			return nil, ValidationError{
				Path:    fmt.Sprintf("pipeline.stages[%d].kind", groupIndex),
				Message: fmt.Sprintf("unsupported stage kind %q", stage.Kind),
			}
		}
	}

	if len(previousGroup) > 0 {
		plan.FinalNode = previousGroup[len(previousGroup)-1]
	}

	return plan, nil
}

func compileOpStage(stage Stage, dependsOn []string, resilienceCfg *resilient.Config) (CompiledNode, error) {
	kind, err := validateHomogeneousOps(stage.Ops, "stage.ops")
	if err != nil {
		return CompiledNode{}, err
	}

	nodeKind := nodeKindForStage(stage.Kind, kind)

	return CompiledNode{
		ID:         stage.Name,
		StageName:  stage.Name,
		Kind:       nodeKind,
		DependsOn:  append([]string(nil), dependsOn...),
		Resilience: cloneResilienceForNode(nodeKind, resilienceCfg),
		Ops:        append([]op.Op(nil), stage.Ops...),
	}, nil
}

func nodeKindForStage(stageKind StageKind, opKind op.Kind) NodeKind {
	switch stageKind {
	case StageContext:
		switch opKind {
		case op.KindFetch:
			return NodeKindContextFetch
		case op.KindModel:
			return NodeKindContextModel
		default:
			return NodeKindContextMutate
		}
	case StageCandidates:
		switch opKind {
		case op.KindFetch:
			return NodeKindCandidatesFetch
		case op.KindModel:
			return NodeKindCandidatesModel
		default:
			return NodeKindCandidatesMutate
		}
	default:
		return ""
	}
}

func compileRetrieveStage(stage Stage, dependsOn []string, groupOffset int, resilienceCfg *resilient.Config) ([]CompiledNode, []ExecutionGroup, []string, error) {
	parallel, ok := stage.Retrieve.(*retrieve.ParallelSpec)
	if !ok {
		return nil, nil, nil, ValidationError{
			Path:    stage.Name,
			Message: "only retrieve.Parallel(...) is supported in Phase 2",
		}
	}

	nodes := make([]CompiledNode, 0, len(parallel.Sources)+1)
	sourceIDs := make([]string, 0, len(parallel.Sources))
	for i, source := range parallel.Sources {
		id := fmt.Sprintf("%s:source:%d", stage.Name, i)
		sourceIDs = append(sourceIDs, id)
		nodes = append(nodes, CompiledNode{
			ID:             id,
			StageName:      stage.Name,
			Kind:           NodeKindRetrieveSource,
			DependsOn:      append([]string(nil), dependsOn...),
			Resilience:     cloneResilienceForNode(NodeKindRetrieveSource, resilienceCfg),
			RetrieveSource: source,
		})
	}

	mergeSpec := parallel.MergeSpec
	nodes = append(nodes, CompiledNode{
		ID:        stage.Name + ":merge",
		StageName: stage.Name,
		Kind:      NodeKindRetrieveMerge,
		DependsOn: append([]string(nil), sourceIDs...),
		MergeSpec: cloneMergeSpec(mergeSpec),
	})

	groups := []ExecutionGroup{
		{
			Index:   groupOffset,
			NodeIDs: append([]string(nil), sourceIDs...),
		},
		{
			Index:   groupOffset + 1,
			NodeIDs: []string{stage.Name + ":merge"},
		},
	}

	return nodes, groups, []string{stage.Name + ":merge"}, nil
}

func cloneMergeSpec(spec merge.Spec) *merge.Spec {
	copied := spec
	return &copied
}

func cloneResilienceForNode(kind NodeKind, cfg *resilient.Config) *resilient.Config {
	if cfg == nil || !resilienceEligible(kind) {
		return nil
	}
	copied := *cfg
	return &copied
}

func resilienceEligible(kind NodeKind) bool {
	switch kind {
	case NodeKindContextFetch, NodeKindContextModel, NodeKindRetrieveSource, NodeKindCandidatesFetch, NodeKindCandidatesModel:
		return true
	default:
		return false
	}
}
