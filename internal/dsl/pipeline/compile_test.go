package pipeline_test

import (
	"testing"
	"time"

	"github.com/dmitryBe/weaver/internal/dsl/expr"
	"github.com/dmitryBe/weaver/internal/dsl/input"
	"github.com/dmitryBe/weaver/internal/dsl/merge"
	"github.com/dmitryBe/weaver/internal/dsl/op"
	"github.com/dmitryBe/weaver/internal/dsl/pipeline"
	"github.com/dmitryBe/weaver/internal/dsl/resilient"
	"github.com/dmitryBe/weaver/internal/dsl/retrieve"
	_ "github.com/dmitryBe/weaver/pipelines"
)

func TestCompileTrendingPlan(t *testing.T) {
	if err := pipeline.DefaultRegistry.BuildAll(); err != nil {
		t.Fatalf("BuildAll failed: %v", err)
	}

	registered, ok := pipeline.DefaultRegistry.Get("sandbox.trending")
	if !ok {
		t.Fatal("expected sandbox.trending in default registry")
	}

	plan := registered.Plan
	if plan == nil {
		t.Fatal("expected compiled plan")
	}

	if got, want := registered.Spec.Name, "sandbox.trending"; got != want {
		t.Fatalf("unexpected spec name: got %q want %q", got, want)
	}

	if got, want := plan.SpecName, "sandbox.trending"; got != want {
		t.Fatalf("unexpected plan spec name: got %q want %q", got, want)
	}

	if got, want := len(plan.Nodes), 10; got != want {
		t.Fatalf("unexpected node count: got %d want %d", got, want)
	}

	if got, want := len(plan.Groups), 9; got != want {
		t.Fatalf("unexpected group count: got %d want %d", got, want)
	}

	if got, want := plan.FinalNode, "post_rank"; got != want {
		t.Fatalf("unexpected final node: got %q want %q", got, want)
	}

	expectedGroups := [][]string{
		{"ctx_fetch"},
		{"ctx_embed"},
		{"ctx_prepare"},
		{"retrieve_candidates:source:0", "retrieve_candidates:source:1"},
		{"retrieve_candidates:merge"},
		{"cand_fetch"},
		{"cand_prepare"},
		{"cand_score"},
		{"post_rank"},
	}

	for i, want := range expectedGroups {
		group := plan.Groups[i]
		if len(group.NodeIDs) != len(want) {
			t.Fatalf("group %d size mismatch: got %d want %d", i, len(group.NodeIDs), len(want))
		}
		for j := range want {
			if got := group.NodeIDs[j]; got != want[j] {
				t.Fatalf("group %d node %d mismatch: got %q want %q", i, j, got, want[j])
			}
		}
	}

	expectedKinds := map[string]pipeline.NodeKind{
		"ctx_fetch":                    pipeline.NodeKindContextFetch,
		"ctx_embed":                    pipeline.NodeKindContextModel,
		"ctx_prepare":                  pipeline.NodeKindContextMutate,
		"retrieve_candidates:source:0": pipeline.NodeKindRetrieveSource,
		"retrieve_candidates:source:1": pipeline.NodeKindRetrieveSource,
		// "retrieve_candidates:source:2": pipeline.NodeKindRetrieveSource,
		"retrieve_candidates:merge": pipeline.NodeKindRetrieveMerge,
		"cand_fetch":                pipeline.NodeKindCandidatesFetch,
		"cand_prepare":              pipeline.NodeKindCandidatesMutate,
		"cand_score":                pipeline.NodeKindCandidatesModel,
		"post_rank":                 pipeline.NodeKindCandidatesMutate,
	}

	nodesByID := make(map[string]pipeline.CompiledNode, len(plan.Nodes))
	for _, node := range plan.Nodes {
		nodesByID[node.ID] = node
	}

	for id, want := range expectedKinds {
		node, ok := nodesByID[id]
		if !ok {
			t.Fatalf("missing node %q", id)
		}
		if node.Kind != want {
			t.Fatalf("unexpected node kind for %q: got %q want %q", id, node.Kind, want)
		}
	}

	mergeNode := nodesByID["retrieve_candidates:merge"]
	if len(mergeNode.DependsOn) != 2 {
		t.Fatalf("unexpected merge dependency count: got %d want %d", len(mergeNode.DependsOn), 3)
	}
}

func TestCompileAppliesPipelineResilienceToEligibleNodes(t *testing.T) {
	spec := pipeline.New("demo").
		Input(input.String("user_id")).
		Resilience(resilient.Default(3*time.Second, 2)).
		Context("ctx_fetch", op.Fetch("user/segment").Key(expr.Context("user_id")).Into("segment")).
		Context("ctx_mutate", op.Set("segment_copy", expr.Context("segment")).String()).
		Retrieve(
			"retrieve",
			retrieve.Parallel(
				retrieve.KV("trending/brands").Key(expr.Context("user_id")).TopK(5),
			).Merge(merge.Default().DedupBy("brand_id")),
		).
		Candidates("cand_fetch", op.Fetch("brand/rating").Key(expr.Candidate("brand_id")).Into("rating").Float()).
		Candidates("cand_model", op.Model("rank").Endpoint("http://localhost").Into("score"))

	plan, err := pipeline.Compile(spec)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	nodesByID := make(map[string]pipeline.CompiledNode, len(plan.Nodes))
	for _, node := range plan.Nodes {
		nodesByID[node.ID] = node
	}

	for _, id := range []string{"ctx_fetch", "retrieve:source:0", "cand_fetch", "cand_model"} {
		node, ok := nodesByID[id]
		if !ok {
			t.Fatalf("missing node %q", id)
		}
		if node.Resilience == nil {
			t.Fatalf("expected resilience on node %q", id)
		}
		if got, want := node.Resilience.MaxLatency, 3*time.Second; got != want {
			t.Fatalf("unexpected max latency on %q: got %s want %s", id, got, want)
		}
		if got, want := node.Resilience.MaxRetries, 2; got != want {
			t.Fatalf("unexpected max retries on %q: got %d want %d", id, got, want)
		}
	}

	for _, id := range []string{"ctx_mutate", "retrieve:merge"} {
		node, ok := nodesByID[id]
		if !ok {
			t.Fatalf("missing node %q", id)
		}
		if node.Resilience != nil {
			t.Fatalf("did not expect resilience on node %q", id)
		}
	}
}
