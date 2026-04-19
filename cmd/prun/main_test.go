package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/dmitryBe/weaver/internal/adapters/featurestore"
	"github.com/dmitryBe/weaver/internal/dsl/expr"
	"github.com/dmitryBe/weaver/internal/dsl/input"
	"github.com/dmitryBe/weaver/internal/dsl/merge"
	"github.com/dmitryBe/weaver/internal/dsl/op"
	"github.com/dmitryBe/weaver/internal/dsl/pipeline"
	"github.com/dmitryBe/weaver/internal/dsl/retrieve"
	"github.com/dmitryBe/weaver/internal/nodes"
	"github.com/dmitryBe/weaver/internal/runner"
	"github.com/dmitryBe/weaver/internal/runtime"
)

func TestParseArgsParsesContextValues(t *testing.T) {
	opts, err := parseArgs([]string{
		"test_suite.trending_cli",
		"--config", "configs/test.yaml",
		"--request-id", "req-1",
		"--debug",
		"--context",
		"city=almaty",
		"vertical=food",
		"user_id=42",
		"limit=10",
		"enabled=true",
		`weights=[1,2]`,
	})
	if err != nil {
		t.Fatalf("parse args: %v", err)
	}

	if got, want := opts.Pipeline, "test_suite.trending_cli"; got != want {
		t.Fatalf("unexpected pipeline: got %q want %q", got, want)
	}
	if got, want := opts.ConfigPath, "configs/test.yaml"; got != want {
		t.Fatalf("unexpected config path: got %q want %q", got, want)
	}
	if got, want := opts.RequestID, "req-1"; got != want {
		t.Fatalf("unexpected request id: got %q want %q", got, want)
	}
	if !opts.Debug {
		t.Fatal("expected debug flag to be parsed")
	}
	if got, want := opts.Context["city"], "almaty"; got != want {
		t.Fatalf("unexpected city: got %#v want %#v", got, want)
	}
	if got, want := opts.Context["user_id"], "42"; got != want {
		t.Fatalf("unexpected user_id: got %#v want %#v", got, want)
	}
	if got, want := opts.Context["enabled"], true; got != want {
		t.Fatalf("unexpected enabled: got %#v want %#v", got, want)
	}
	weights, ok := opts.Context["weights"].([]any)
	if !ok {
		t.Fatalf("expected parsed array, got %#v", opts.Context["weights"])
	}
	if got, want := len(weights), 2; got != want {
		t.Fatalf("unexpected weights len: got %d want %d", got, want)
	}
}

func TestParseArgsRejectsEmptyContextBlock(t *testing.T) {
	_, err := parseArgs([]string{"test_suite.trending_cli", "--context"})
	if err == nil || err.Error() != "--context requires one or more key=value pairs" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestExecutePipelineReturnsCandidatesOnly(t *testing.T) {
	current := runnerForPRunTest(t)

	executed, err := current.Execute(
		context.Background(),
		runner.ExecuteRequest{
			Pipeline:  "test_suite.trending_cli",
			RequestID: "req-123",
			Context: map[string]any{
				"city":     "almaty",
				"vertical": "food",
			},
			NormalizeInput: true,
		},
	)
	if err != nil {
		t.Fatalf("execute pipeline: %v", err)
	}

	result := output{
		RequestID:  executed.RequestID,
		Pipeline:   executed.Pipeline,
		Candidates: toResponse(executed.State.Candidates),
	}

	if got, want := result.RequestID, "req-123"; got != want {
		t.Fatalf("unexpected request id: got %q want %q", got, want)
	}
	if got, want := result.Pipeline, "test_suite.trending_cli"; got != want {
		t.Fatalf("unexpected pipeline: got %q want %q", got, want)
	}
	if got, want := len(result.Candidates), 2; got != want {
		t.Fatalf("unexpected candidate count: got %d want %d", got, want)
	}
	if got := result.Candidates[0]["brand_id"]; got != float64(101) {
		t.Fatalf("unexpected first candidate brand_id: got %#v", got)
	}
	if _, ok := result.Candidates[0]["rating"]; !ok {
		t.Fatalf("expected fetched rating on candidate: %#v", result.Candidates[0])
	}
}

func TestRunWithOptionsIncludesDebugSnapshots(t *testing.T) {
	current := runnerForPRunTest(t)

	executed, err := current.Execute(
		context.Background(),
		runner.ExecuteRequest{
			Pipeline: "test_suite.trending_cli",
			Context: map[string]any{
				"city":     "almaty",
				"vertical": "food",
			},
			NormalizeInput: true,
			Debug:          true,
		},
	)
	if err != nil {
		t.Fatalf("execute debug pipeline: %v", err)
	}

	result := output{
		RequestID:  executed.RequestID,
		Pipeline:   executed.Pipeline,
		Candidates: toResponse(executed.State.Candidates),
		DebugInfo:  executed.DebugInfo,
	}

	if result.DebugInfo == nil {
		t.Fatal("expected debug info")
	}
	if got, want := len(result.DebugInfo.Steps), 4; got != want {
		t.Fatalf("unexpected debug step count: got %d want %d", got, want)
	}
	if got, want := result.DebugInfo.Steps[0].StageName, "ctx_prepare"; got != want {
		t.Fatalf("unexpected first stage: got %q want %q", got, want)
	}
	if got, want := result.DebugInfo.Steps[1].NodeID, "retrieve_candidates:merge"; got != want {
		t.Fatalf("unexpected retrieve snapshot node: got %q want %q", got, want)
	}
	if _, ok := result.DebugInfo.Steps[1].Candidates[0]["rating"]; ok {
		t.Fatalf("retrieve snapshot should not include fetched rating yet: %#v", result.DebugInfo.Steps[1].Candidates[0])
	}
	if _, ok := result.DebugInfo.Steps[2].Candidates[0]["rating"]; !ok {
		t.Fatalf("candidate fetch snapshot should include rating: %#v", result.DebugInfo.Steps[2].Candidates[0])
	}
}

func runnerForPRunTest(t *testing.T) *runner.Runner {
	t.Helper()
	registry := pipeline.NewRegistry()
	registry.Register("test_suite", "trending_cli", func() *pipeline.Spec {
		return pipeline.New().
			Input(
				input.String("city"),
				input.String("vertical"),
			).
			Context(
				"ctx_prepare",
				op.Set("city_copy", expr.Context("city")).String(),
			).
			Retrieve(
				"retrieve_candidates",
				retrieve.Parallel(
					retrieve.KV("trending/brands").
						Key(expr.Tuple(expr.Context("city"), expr.Context("vertical"))).
						TopK(10),
				).Merge(
					merge.Default().DedupBy("brand_id").SortByScore("score"),
				),
			).
			Candidates(
				"cand_fetch",
				op.Fetch("brand/rating").
					Key(expr.Candidate("brand_id")).
					Into("rating").
					Float().
					Default(0),
			).
			Candidates(
				"post_rank",
				op.Take(2),
			)
	})
	if err := registry.BuildAll(); err != nil {
		t.Fatalf("BuildAll failed: %v", err)
	}

	nodeRegistry := runtime.NewRegistry()
	nodes.RegisterDefaults(nodeRegistry)

	return &runner.Runner{
		Registry: registry,
		Engine:   runtime.NewEngine(nodeRegistry),
		Env: &runtime.ExecEnv{
			FeatureStore: featurestore.NewFileStore(filepath.Join("..", "..", "testdata", "featurestore", "unit")),
		},
	}
}
