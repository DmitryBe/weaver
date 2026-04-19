package nodes

import (
	"context"
	"testing"

	"github.com/dmitryBe/weaver/internal/dsl/expr"
	"github.com/dmitryBe/weaver/internal/dsl/merge"
	"github.com/dmitryBe/weaver/internal/dsl/op"
	"github.com/dmitryBe/weaver/internal/dsl/pipeline"
	"github.com/dmitryBe/weaver/internal/runtime"
)

func TestCandidatesMutateExecutorDoesNotMutateInputState(t *testing.T) {
	executor := CandidatesMutateExecutor{}
	input := runtime.NodeInput{
		State: runtime.State{
			Context: runtime.Context{"user_id": "u1"},
			Candidates: []runtime.Candidate{
				{"brand_id": 101},
				{"brand_id": 102},
			},
		},
	}
	node := pipeline.CompiledNode{
		ID:   "cand_mutate",
		Kind: pipeline.NodeKindCandidatesMutate,
		Ops: []op.Op{
			op.Set("user_id", expr.Context("user_id")).String(),
			op.SortBy("brand_id", op.Desc),
			op.Take(1),
		},
	}

	out, err := executor.Execute(context.Background(), node, input, &runtime.ExecEnv{})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if got, want := len(input.State.Candidates), 2; got != want {
		t.Fatalf("unexpected input candidate count: got %d want %d", got, want)
	}
	if _, ok := input.State.Candidates[0]["user_id"]; ok {
		t.Fatalf("input candidate mutated unexpectedly: %#v", input.State.Candidates[0])
	}
	if got, want := input.State.Candidates[0]["brand_id"], 101; got != want {
		t.Fatalf("unexpected input candidate order: got %#v want %#v", got, want)
	}
	if got, want := out.Candidates[0]["brand_id"], 102; got != want {
		t.Fatalf("unexpected output candidate brand_id: got %#v want %#v", got, want)
	}
	if got, want := out.Candidates[0]["user_id"], "u1"; got != want {
		t.Fatalf("unexpected output candidate user_id: got %#v want %#v", got, want)
	}
}

func TestCandidatesMutateExecutorSupportsDrop(t *testing.T) {
	executor := CandidatesMutateExecutor{}
	input := runtime.NodeInput{
		State: runtime.State{
			Candidates: []runtime.Candidate{
				{"brand_id": 101, "rating": 4.8, "score": 0.9, "debug": "x"},
				{"brand_id": 102, "rating": 4.5, "score": 0.4, "debug": "y"},
			},
		},
	}
	node := pipeline.CompiledNode{
		ID:   "post_rank",
		Kind: pipeline.NodeKindCandidatesMutate,
		Ops: []op.Op{
			op.SortBy("score", op.Desc),
			op.Take(1),
			op.Drop("rating", "debug"),
		},
	}

	out, err := executor.Execute(context.Background(), node, input, &runtime.ExecEnv{})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if got, want := len(out.Candidates), 1; got != want {
		t.Fatalf("unexpected output candidate count: got %d want %d", got, want)
	}
	if _, ok := out.Candidates[0]["rating"]; ok {
		t.Fatalf("expected rating to be dropped: %#v", out.Candidates[0])
	}
	if _, ok := out.Candidates[0]["debug"]; ok {
		t.Fatalf("expected debug to be dropped: %#v", out.Candidates[0])
	}
	if got, want := out.Candidates[0]["score"], 0.9; got != want {
		t.Fatalf("unexpected output candidate score: got %#v want %#v", got, want)
	}
	if got, want := input.State.Candidates[0]["rating"], 4.8; got != want {
		t.Fatalf("input candidate mutated unexpectedly: got %#v want %#v", got, want)
	}
}

func TestCandidatesMutateExecutorSupportsDropExcept(t *testing.T) {
	executor := CandidatesMutateExecutor{}
	input := runtime.NodeInput{
		State: runtime.State{
			Candidates: []runtime.Candidate{
				{"brand_id": 101, "rating": 4.8, "score": 0.9, "name": "A"},
			},
		},
	}
	node := pipeline.CompiledNode{
		ID:   "post_rank",
		Kind: pipeline.NodeKindCandidatesMutate,
		Ops: []op.Op{
			op.DropExcept("brand_id", "score"),
		},
	}

	out, err := executor.Execute(context.Background(), node, input, &runtime.ExecEnv{})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if got, want := len(out.Candidates[0]), 2; got != want {
		t.Fatalf("unexpected output field count: got %d want %d", got, want)
	}
	if got, want := out.Candidates[0]["brand_id"], 101; got != want {
		t.Fatalf("unexpected brand_id: got %#v want %#v", got, want)
	}
	if got, want := out.Candidates[0]["score"], 0.9; got != want {
		t.Fatalf("unexpected score: got %#v want %#v", got, want)
	}
	if _, ok := out.Candidates[0]["rating"]; ok {
		t.Fatalf("expected rating to be removed: %#v", out.Candidates[0])
	}
	if got, want := input.State.Candidates[0]["name"], "A"; got != want {
		t.Fatalf("input candidate mutated unexpectedly: got %#v want %#v", got, want)
	}
}

func TestContextMutateExecutorSupportsDropAndDropExcept(t *testing.T) {
	executor := ContextMutateExecutor{}
	input := runtime.NodeInput{
		State: runtime.State{
			Context: runtime.Context{
				"user_id": "u1",
				"city":    "almaty",
				"debug":   true,
				"extra":   "x",
			},
		},
	}
	node := pipeline.CompiledNode{
		ID:   "ctx_mutate",
		Kind: pipeline.NodeKindContextMutate,
		Ops: []op.Op{
			op.Drop("debug", "extra"),
			op.DropExcept("user_id", "city"),
		},
	}

	out, err := executor.Execute(context.Background(), node, input, &runtime.ExecEnv{})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if got, want := len(out.Context), 2; got != want {
		t.Fatalf("unexpected context field count: got %d want %d", got, want)
	}
	if got, want := out.Context["user_id"], "u1"; got != want {
		t.Fatalf("unexpected user_id: got %#v want %#v", got, want)
	}
	if got, want := out.Context["city"], "almaty"; got != want {
		t.Fatalf("unexpected city: got %#v want %#v", got, want)
	}
	if _, ok := out.Context["debug"]; ok {
		t.Fatalf("expected debug to be removed: %#v", out.Context)
	}
	if got, want := input.State.Context["extra"], "x"; got != want {
		t.Fatalf("input context mutated unexpectedly: got %#v want %#v", got, want)
	}
}

func TestRetrieveMergeExecutorDoesNotMutateUpstreamOutputs(t *testing.T) {
	executor := RetrieveMergeExecutor{}
	spec := merge.Default().DedupBy("brand_id").SortByScore("score")
	input := runtime.NodeInput{
		Upstreams: map[string]runtime.NodeOutput{
			"left": {
				Candidates: []runtime.Candidate{
					{"brand_id": 101, "score": 0.3},
					{"brand_id": 102, "score": 0.9},
				},
			},
			"right": {
				Candidates: []runtime.Candidate{
					{"brand_id": 101, "score": 0.8},
				},
			},
		},
	}
	node := pipeline.CompiledNode{
		ID:        "retrieve:merge",
		Kind:      pipeline.NodeKindRetrieveMerge,
		DependsOn: []string{"left", "right"},
		MergeSpec: &spec,
	}

	out, err := executor.Execute(context.Background(), node, input, &runtime.ExecEnv{})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if got, want := len(out.Candidates), 2; got != want {
		t.Fatalf("unexpected merged candidate count: got %d want %d", got, want)
	}
	if got, want := out.Candidates[0]["brand_id"], 102; got != want {
		t.Fatalf("unexpected top merged brand_id: got %#v want %#v", got, want)
	}
	if got, want := input.Upstreams["left"].Candidates[0]["brand_id"], 101; got != want {
		t.Fatalf("left upstream mutated unexpectedly: got %#v want %#v", got, want)
	}
	if got, want := input.Upstreams["left"].Candidates[1]["brand_id"], 102; got != want {
		t.Fatalf("left upstream order changed unexpectedly: got %#v want %#v", got, want)
	}
	if got, want := input.Upstreams["right"].Candidates[0]["score"], 0.8; got != want {
		t.Fatalf("right upstream mutated unexpectedly: got %#v want %#v", got, want)
	}
}
