package nodes

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/dmitryBe/weaver/internal/adapters/featurestore"
	"github.com/dmitryBe/weaver/internal/dsl/expr"
	"github.com/dmitryBe/weaver/internal/dsl/op"
	"github.com/dmitryBe/weaver/internal/dsl/pipeline"
	"github.com/dmitryBe/weaver/internal/metrics"
	"github.com/dmitryBe/weaver/internal/runtime"
	"github.com/dmitryBe/weaver/internal/testutil"
)

func TestContextFetchExecutorAppliesDefaults(t *testing.T) {
	executor := ContextFetchExecutor{}
	node := pipeline.CompiledNode{
		ID:   "ctx_fetch",
		Kind: pipeline.NodeKindContextFetch,
		Ops: []op.Op{
			op.Fetch("user/segment").Key(expr.Context("user_id")).Into("segment").Float(),
			op.Fetch("user/missing").Key(expr.Context("user_id")).Into("missing").Int().Default(99),
		},
	}

	out, err := executor.Execute(
		context.Background(),
		node,
		runtime.NodeInput{
			State: runtime.State{
				Context: runtime.Context{"user_id": "u1"},
			},
		},
		&runtime.ExecEnv{
			FeatureStore: fakeNodeFeatureStore{
				rows: []map[string]any{{"user/segment": 0.75}},
			},
		},
	)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if got, want := out.Context["segment"], 0.75; got != want {
		t.Fatalf("unexpected segment: got %v want %v", got, want)
	}
	if got, want := out.Context["missing"], 99; got != want {
		t.Fatalf("unexpected defaulted field: got %v want %v", got, want)
	}
}

func TestContextFetchExecutorSplitsByEntity(t *testing.T) {
	executor := ContextFetchExecutor{}
	store := testutil.NewCountingFeatureStore(featurestore.NewFileStore(filepath.Join("..", "..", "testdata", "featurestore", "unit")))
	node := pipeline.CompiledNode{
		ID:   "ctx_fetch",
		Kind: pipeline.NodeKindContextFetch,
		Ops: []op.Op{
			op.Fetch("user/segment").Key(expr.Context("user_id")).Into("segment").Float(),
			op.Fetch("brand/rating").Key(expr.Context("brand_id")).Into("brand_rating").Float(),
		},
	}

	out, err := executor.Execute(
		context.Background(),
		node,
		runtime.NodeInput{
			State: runtime.State{
				Context: runtime.Context{
					"user_id":  "u1",
					"brand_id": 101,
				},
			},
		},
		&runtime.ExecEnv{
			FeatureStore: store,
		},
	)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if got, want := out.Context["segment"], 0.75; got != want {
		t.Fatalf("unexpected segment: got %v want %v", got, want)
	}
	if got, want := out.Context["brand_rating"], 4.8; got != want {
		t.Fatalf("unexpected brand rating: got %v want %v", got, want)
	}
	if got, want := store.Calls(), 2; got != want {
		t.Fatalf("unexpected feature store call count: got %d want %d", got, want)
	}
}

func TestContextFetchExecutorRunsEntityFetchesInParallel(t *testing.T) {
	executor := ContextFetchExecutor{}
	store := testutil.NewCountingFeatureStore(featurestore.NewFileStore(filepath.Join("..", "..", "testdata", "featurestore", "unit")))
	store.Delay = 50 * time.Millisecond
	node := pipeline.CompiledNode{
		ID:   "ctx_fetch",
		Kind: pipeline.NodeKindContextFetch,
		Ops: []op.Op{
			op.Fetch("user/segment").Key(expr.Context("user_id")).Into("segment").Float(),
			op.Fetch("brand/rating").Key(expr.Context("brand_id")).Into("brand_rating").Float(),
		},
	}

	out, err := executor.Execute(
		context.Background(),
		node,
		runtime.NodeInput{
			State: runtime.State{
				Context: runtime.Context{
					"user_id":  "u1",
					"brand_id": 101,
				},
			},
		},
		&runtime.ExecEnv{
			FeatureStore:                    store,
			FeatureStoreFetchMaxParallelism: 2,
		},
	)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if got, want := out.Context["segment"], 0.75; got != want {
		t.Fatalf("unexpected segment: got %v want %v", got, want)
	}
	if got, want := out.Context["brand_rating"], 4.8; got != want {
		t.Fatalf("unexpected brand rating: got %v want %v", got, want)
	}
	if got, want := store.Calls(), 2; got != want {
		t.Fatalf("unexpected feature store call count: got %d want %d", got, want)
	}
	if got := store.MaxInFlight(); got < 2 {
		t.Fatalf("expected parallel entity fetches, got max in-flight %d", got)
	}
	if got := store.MaxInFlight(); got > 2 {
		t.Fatalf("expected max in-flight <= 2, got %d", got)
	}
}

func TestContextFetchExecutorDeduplicatesRepeatedSourceFeature(t *testing.T) {
	executor := ContextFetchExecutor{}
	store := testutil.NewCountingFeatureStore(featurestore.NewFileStore(filepath.Join("..", "..", "testdata", "featurestore", "unit")))
	node := pipeline.CompiledNode{
		ID:   "ctx_fetch",
		Kind: pipeline.NodeKindContextFetch,
		Ops: []op.Op{
			op.Fetch("user/segment").Key(expr.Context("user_id")).Into("segment").Float(),
			op.Fetch("user/segment").Key(expr.Context("user_id")).Into("segment_copy").Float(),
		},
	}

	out, err := executor.Execute(
		context.Background(),
		node,
		runtime.NodeInput{
			State: runtime.State{
				Context: runtime.Context{"user_id": "u1"},
			},
		},
		&runtime.ExecEnv{
			FeatureStore: store,
		},
	)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if got, want := out.Context["segment"], 0.75; got != want {
		t.Fatalf("unexpected segment: got %v want %v", got, want)
	}
	if got, want := out.Context["segment_copy"], 0.75; got != want {
		t.Fatalf("unexpected segment_copy: got %v want %v", got, want)
	}
	if got, want := store.Calls(), 1; got != want {
		t.Fatalf("unexpected feature store call count: got %d want %d", got, want)
	}
	logs := store.Logs()
	if got, want := len(logs[0].Features), 1; got != want {
		t.Fatalf("unexpected feature count in request: got %d want %d", got, want)
	}
}

func TestContextFetchExecutorRecordsDefaultMetricsForNullAndMissingValues(t *testing.T) {
	executor := ContextFetchExecutor{}
	recorder := metrics.NewTestRecorder([]string{"user/segment"})
	node := pipeline.CompiledNode{
		ID:        "ctx_fetch",
		StageName: "ctx_fetch",
		Kind:      pipeline.NodeKindContextFetch,
		Ops: []op.Op{
			op.Fetch("user/segment").Key(expr.Context("user_id")).Into("segment").Float().Default(42),
			op.Fetch("user/missing").Key(expr.Context("user_id")).Into("missing").Int().Default(99),
		},
	}

	out, err := executor.Execute(
		context.Background(),
		node,
		runtime.NodeInput{
			State: runtime.State{
				Context: runtime.Context{"user_id": "u1"},
				Meta:    runtime.Meta{Pipeline: "ctx_metrics"},
			},
		},
		&runtime.ExecEnv{
			FeatureStore: fakeNodeFeatureStore{
				rows: []map[string]any{{"user/segment": nil}},
			},
			Metrics: recorder,
		},
	)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if got, want := out.Context["segment"], 42.0; got != want {
		t.Fatalf("unexpected defaulted segment: got %v want %v", got, want)
	}
	if got, want := out.Context["missing"], 99; got != want {
		t.Fatalf("unexpected defaulted missing: got %v want %v", got, want)
	}
	if got, want := len(recorder.FeatureDefaults), 2; got != want {
		t.Fatalf("unexpected feature default metric count: got %d want %d", got, want)
	}
	if got, want := recorder.FeatureDefaults[0].Feature, "user/segment"; got != want {
		t.Fatalf("unexpected allowlisted feature dimension: got %q want %q", got, want)
	}
	if got, want := recorder.FeatureDefaults[0].Reason, metrics.ReasonNull; got != want {
		t.Fatalf("unexpected null default reason: got %q want %q", got, want)
	}
	if got := recorder.FeatureDefaults[1].Feature; got != "" {
		t.Fatalf("expected non-allowlisted feature to be blank, got %q", got)
	}
	if got, want := recorder.FeatureDefaults[1].Reason, metrics.ReasonMissing; got != want {
		t.Fatalf("unexpected missing default reason: got %q want %q", got, want)
	}
}
