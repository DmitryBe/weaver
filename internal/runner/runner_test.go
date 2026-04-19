package runner

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	cacheadapter "github.com/dmitryBe/weaver/internal/adapters/cache"
	"github.com/dmitryBe/weaver/internal/adapters/featurestore"
	dslcache "github.com/dmitryBe/weaver/internal/dsl/cache"
	"github.com/dmitryBe/weaver/internal/dsl/expr"
	"github.com/dmitryBe/weaver/internal/dsl/input"
	"github.com/dmitryBe/weaver/internal/dsl/merge"
	"github.com/dmitryBe/weaver/internal/dsl/op"
	"github.com/dmitryBe/weaver/internal/dsl/pipeline"
	"github.com/dmitryBe/weaver/internal/dsl/retrieve"
	"github.com/dmitryBe/weaver/internal/nodes"
	"github.com/dmitryBe/weaver/internal/runtime"
	"github.com/dmitryBe/weaver/internal/testutil"
)

func TestExecuteRunsPipeline(t *testing.T) {
	current := newTestRunner(t)

	result, err := current.Execute(context.Background(), ExecuteRequest{
		Pipeline:  "test_suite.trending_runner",
		RequestID: "req-123",
		Context: map[string]any{
			"city":     "almaty",
			"vertical": "food",
		},
	})
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if got, want := result.RequestID, "req-123"; got != want {
		t.Fatalf("unexpected request id: got %q want %q", got, want)
	}
	if got, want := result.Pipeline, "test_suite.trending_runner"; got != want {
		t.Fatalf("unexpected pipeline: got %q want %q", got, want)
	}
	if got, want := result.State.Context["city_copy"], "almaty"; got != want {
		t.Fatalf("unexpected context value: got %#v want %#v", got, want)
	}
	if got, want := len(result.State.Candidates), 2; got != want {
		t.Fatalf("unexpected candidate count: got %d want %d", got, want)
	}
}

func TestExecuteReturnsPipelineNotFound(t *testing.T) {
	current := newTestRunner(t)

	_, err := current.Execute(context.Background(), ExecuteRequest{Pipeline: "missing.pipeline"})
	if !IsPipelineNotFound(err) {
		t.Fatalf("expected pipeline not found error, got %v", err)
	}
}

func TestExecuteReturnsInvalidInputForMissingField(t *testing.T) {
	current := newTestRunner(t)

	_, err := current.Execute(context.Background(), ExecuteRequest{
		Pipeline: "test_suite.trending_runner",
		Context: map[string]any{
			"city": "almaty",
		},
	})
	if !IsInvalidInput(err) {
		t.Fatalf("expected invalid input error, got %v", err)
	}
}

func TestExecuteReturnsInvalidInputForWrongType(t *testing.T) {
	current := newTypedRunner(t)

	_, err := current.Execute(context.Background(), ExecuteRequest{
		Pipeline: "test_suite.typed_runner",
		Context: map[string]any{
			"limit": "abc",
		},
	})
	if !IsInvalidInput(err) {
		t.Fatalf("expected invalid input error, got %v", err)
	}
}

func TestExecuteGeneratesRequestIDWhenMissing(t *testing.T) {
	current := newTestRunner(t)

	result, err := current.Execute(context.Background(), ExecuteRequest{
		Pipeline: "test_suite.trending_runner",
		Context: map[string]any{
			"city":     "almaty",
			"vertical": "food",
		},
	})
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if result.RequestID == "" {
		t.Fatal("expected generated request id")
	}
}

func TestExecuteCanNormalizeInputs(t *testing.T) {
	current := newTypedRunner(t)

	result, err := current.Execute(context.Background(), ExecuteRequest{
		Pipeline:       "test_suite.typed_runner",
		NormalizeInput: true,
		Context: map[string]any{
			"limit": "12",
		},
	})
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if got, want := result.State.Context["limit_copy"], 12; got != want {
		t.Fatalf("unexpected normalized value: got %#v want %#v", got, want)
	}
}

func TestExecuteValidatesCandidateInputSchema(t *testing.T) {
	current := newCandidateSchemaRunner(t)

	_, err := current.Execute(context.Background(), ExecuteRequest{
		Pipeline: "test_suite.candidate_schema_runner",
		Context: map[string]any{
			"user_id": "u1",
		},
		Candidates: []map[string]any{
			{"id": "123"},
		},
	})
	if !IsInvalidInput(err) {
		t.Fatalf("expected invalid input error, got %v", err)
	}
}

func TestExecuteCanNormalizeCandidateInputs(t *testing.T) {
	current := newCandidateSchemaRunner(t)

	result, err := current.Execute(context.Background(), ExecuteRequest{
		Pipeline:       "test_suite.candidate_schema_runner",
		NormalizeInput: true,
		Context: map[string]any{
			"user_id": "u1",
		},
		Candidates: []map[string]any{
			{
				"id":    "123",
				"score": "7",
			},
		},
	})
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if got, want := result.State.Candidates[0]["score"], 7; got != want {
		t.Fatalf("unexpected normalized candidate value: got %#v want %#v", got, want)
	}
}

func TestExecuteUsesPipelineCache(t *testing.T) {
	current, store := newCachedRunner(t, 5*time.Minute)

	first, err := current.Execute(context.Background(), ExecuteRequest{
		Pipeline:  "test_suite.cached_runner",
		RequestID: "req-1",
		Context: map[string]any{
			"city":     "almaty",
			"vertical": "food",
		},
	})
	if err != nil {
		t.Fatalf("first execute failed: %v", err)
	}
	if first.CacheHit {
		t.Fatal("expected first request to miss cache")
	}
	firstCalls := store.Calls()
	if firstCalls == 0 {
		t.Fatal("expected first request to hit feature store")
	}

	second, err := current.Execute(context.Background(), ExecuteRequest{
		Pipeline:  "test_suite.cached_runner",
		RequestID: "req-2",
		Context: map[string]any{
			"city":     "almaty",
			"vertical": "food",
		},
	})
	if err != nil {
		t.Fatalf("second execute failed: %v", err)
	}
	if !second.CacheHit {
		t.Fatal("expected second request to hit cache")
	}
	if got, want := store.Calls(), firstCalls; got != want {
		t.Fatalf("expected cached request to skip feature store: got %d want %d", got, want)
	}
	if got, want := second.RequestID, "req-2"; got != want {
		t.Fatalf("unexpected cached request id: got %q want %q", got, want)
	}
	if !reflect.DeepEqual(first.State.Candidates, second.State.Candidates) {
		t.Fatalf("unexpected cached candidates:\n got %#v\nwant %#v", second.State.Candidates, first.State.Candidates)
	}
}

func TestExecuteExpiresPipelineCacheEntries(t *testing.T) {
	current, store := newCachedRunner(t, 15*time.Millisecond)

	first, err := current.Execute(context.Background(), ExecuteRequest{
		Pipeline: "test_suite.cached_runner",
		Context: map[string]any{
			"city":     "almaty",
			"vertical": "food",
		},
	})
	if err != nil {
		t.Fatalf("first execute failed: %v", err)
	}
	if first.CacheHit {
		t.Fatal("expected first request to miss cache")
	}
	firstCalls := store.Calls()

	second, err := current.Execute(context.Background(), ExecuteRequest{
		Pipeline: "test_suite.cached_runner",
		Context: map[string]any{
			"city":     "almaty",
			"vertical": "food",
		},
	})
	if err != nil {
		t.Fatalf("second execute failed: %v", err)
	}
	if !second.CacheHit {
		t.Fatal("expected second request to hit cache")
	}

	time.Sleep(30 * time.Millisecond)

	third, err := current.Execute(context.Background(), ExecuteRequest{
		Pipeline: "test_suite.cached_runner",
		Context: map[string]any{
			"city":     "almaty",
			"vertical": "food",
		},
	})
	if err != nil {
		t.Fatalf("third execute failed: %v", err)
	}
	if third.CacheHit {
		t.Fatal("expected cache entry to expire")
	}
	if got := store.Calls(); got <= firstCalls {
		t.Fatalf("expected expired cache entry to trigger pipeline execution, got %d calls", got)
	}
}

func newTestRunner(t *testing.T) *Runner {
	t.Helper()

	registry := pipeline.NewRegistry()
	registry.Register("test_suite", "trending_runner", func() *pipeline.Spec {
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

	return &Runner{
		Registry: registry,
		Engine:   runtime.NewEngine(nodeRegistry),
		Env: &runtime.ExecEnv{
			FeatureStore: featurestore.NewFileStore(filepath.Join("..", "..", "testdata", "featurestore", "unit")),
		},
	}
}

func newTypedRunner(t *testing.T) *Runner {
	t.Helper()

	registry := pipeline.NewRegistry()
	registry.Register("test_suite", "typed_runner", func() *pipeline.Spec {
		return pipeline.New().
			Input(input.Int("limit")).
			Context(
				"ctx_prepare",
				op.Set("limit_copy", expr.Context("limit")).Int(),
			).
			Candidates("post_rank", op.Take(1))
	})
	if err := registry.BuildAll(); err != nil {
		t.Fatalf("BuildAll failed: %v", err)
	}

	nodeRegistry := runtime.NewRegistry()
	nodes.RegisterDefaults(nodeRegistry)

	return &Runner{
		Registry: registry,
		Engine:   runtime.NewEngine(nodeRegistry),
		Env:      &runtime.ExecEnv{},
	}
}

func newCandidateSchemaRunner(t *testing.T) *Runner {
	t.Helper()

	registry := pipeline.NewRegistry()
	registry.Register("test_suite", "candidate_schema_runner", func() *pipeline.Spec {
		return pipeline.New().
			Input(
				input.String("user_id"),
				input.Candidates(
					input.String("id"),
					input.Int("score"),
				),
			).
			Candidates("post_rank", op.Take(1))
	})
	if err := registry.BuildAll(); err != nil {
		t.Fatalf("BuildAll failed: %v", err)
	}

	nodeRegistry := runtime.NewRegistry()
	nodes.RegisterDefaults(nodeRegistry)

	return &Runner{
		Registry: registry,
		Engine:   runtime.NewEngine(nodeRegistry),
		Env:      &runtime.ExecEnv{},
	}
}

func newCachedRunner(t *testing.T, ttl time.Duration) (*Runner, *testutil.CountingFeatureStore) {
	t.Helper()

	registry := pipeline.NewRegistry()
	registry.Register("test_suite", "cached_runner", func() *pipeline.Spec {
		return pipeline.New().
			Input(
				input.String("city"),
				input.String("vertical"),
			).
			Cache(
				dslcache.Key(expr.Tuple(expr.Context("city"), expr.Context("vertical"))).
					TTL(ttl),
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

	store := testutil.NewCountingFeatureStore(featurestore.NewFileStore(filepath.Join("..", "..", "testdata", "featurestore", "unit")))

	return &Runner{
		Registry: registry,
		Engine:   runtime.NewEngine(nodeRegistry),
		Env: &runtime.ExecEnv{
			FeatureStore: store,
			Cache:        cacheadapter.NewInMemoryLRU(64),
		},
	}, store
}
