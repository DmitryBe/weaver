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
	"github.com/dmitryBe/weaver/internal/nodeutil"
	"github.com/dmitryBe/weaver/internal/runtime"
	"github.com/dmitryBe/weaver/internal/testutil"
)

func TestCandidatesFetchExecutorSplitsByEntity(t *testing.T) {
	executor := CandidatesFetchExecutor{}
	store := testutil.NewCountingFeatureStore(featurestore.NewFileStore(filepath.Join("..", "..", "testdata", "featurestore", "unit")))
	node := pipeline.CompiledNode{
		ID:   "cand_fetch",
		Kind: pipeline.NodeKindCandidatesFetch,
		Ops: []op.Op{
			op.Fetch("user/segment").Key(expr.Context("user_id")).Into("user_segment").Float().Default(0),
			op.Fetch("brand/rating").Key(expr.Candidate("brand_id")).Into("rating").Float().Default(0),
			op.Fetch("brand/price_tier").Key(expr.Candidate("brand_id")).Into("price_tier").Int().Default(0),
			op.Fetch("brand_user/score").Key(expr.Tuple(expr.Candidate("brand_id"), expr.Context("user_id"))).Into("brand_user_score").Float().Default(0),
		},
	}

	out, err := executor.Execute(
		context.Background(),
		node,
		runtime.NodeInput{
			State: runtime.State{
				Context: runtime.Context{"user_id": "u1"},
				Candidates: []runtime.Candidate{
					{"brand_id": 101},
					{"brand_id": 999},
				},
			},
		},
		&runtime.ExecEnv{
			FeatureStore:                    store,
			FeatureStoreFetchMaxParallelism: 1,
		},
	)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if got, want := out.Candidates[0]["rating"], 4.8; got != want {
		t.Fatalf("unexpected first rating: got %v want %v", got, want)
	}
	if got, want := out.Candidates[0]["price_tier"], 2; got != want {
		t.Fatalf("unexpected first price_tier: got %v want %v", got, want)
	}
	if got, want := out.Candidates[0]["user_segment"], 0.75; got != want {
		t.Fatalf("unexpected first user_segment: got %v want %v", got, want)
	}
	if got, want := out.Candidates[0]["brand_user_score"], 0.9; got != want {
		t.Fatalf("unexpected first brand_user_score: got %v want %v", got, want)
	}
	if got, want := out.Candidates[1]["rating"], 0.0; got != want {
		t.Fatalf("unexpected defaulted rating: got %v want %v", got, want)
	}
	if got, want := out.Candidates[1]["price_tier"], 0; got != want {
		t.Fatalf("unexpected defaulted price_tier: got %v want %v", got, want)
	}
	if got, want := out.Candidates[1]["user_segment"], 0.75; got != want {
		t.Fatalf("unexpected second user_segment: got %v want %v", got, want)
	}
	if got, want := out.Candidates[1]["brand_user_score"], 0.0; got != want {
		t.Fatalf("unexpected defaulted brand_user_score: got %v want %v", got, want)
	}
	if got, want := store.Calls(), 3; got != want {
		t.Fatalf("unexpected feature store call count: got %d want %d", got, want)
	}

	logs := store.Logs()
	var userCall *testutil.FeatureStoreCall
	for i := range logs {
		if len(logs[i].Features) == 1 && logs[i].Features[0] == "user/segment" {
			userCall = &logs[i]
			break
		}
	}
	if userCall == nil {
		t.Fatal("expected user/segment feature store call")
	}
	if got, want := len(userCall.Keys), 1; got != want {
		t.Fatalf("unexpected deduped user key count: got %d want %d", got, want)
	}
}

func TestBuildCandidateEntityFetchPlanDeduplicatesContextOnlyKeys(t *testing.T) {
	ops := []*op.FetchOp{
		op.Fetch("user/segment").Key(expr.Context("user_id")).Into("user_segment").Float().Default(0),
	}

	plan, err := buildCandidateEntityFetchPlan(
		ops,
		runtime.State{
			Context: runtime.Context{"user_id": "u1"},
		},
		[]runtime.Candidate{
			{"brand_id": 101},
			{"brand_id": 102},
		},
	)
	if err != nil {
		t.Fatalf("build plan failed: %v", err)
	}

	if got, want := len(plan.uniqueKeys), 1; got != want {
		t.Fatalf("unexpected unique key count: got %d want %d", got, want)
	}
	normalized := nodeutil.NormalizeKey(plan.uniqueKeys[0])
	if got, want := len(plan.targetsByKey[normalized]), 2; got != want {
		t.Fatalf("unexpected target fanout: got %d want %d", got, want)
	}
}

func TestCandidatesFetchExecutorRunsEntityFetchesInParallel(t *testing.T) {
	executor := CandidatesFetchExecutor{}
	store := testutil.NewCountingFeatureStore(featurestore.NewFileStore(filepath.Join("..", "..", "testdata", "featurestore", "unit")))
	store.Delay = 50 * time.Millisecond
	node := pipeline.CompiledNode{
		ID:   "cand_fetch",
		Kind: pipeline.NodeKindCandidatesFetch,
		Ops: []op.Op{
			op.Fetch("user/segment").Key(expr.Context("user_id")).Into("user_segment").Float().Default(0),
			op.Fetch("brand/rating").Key(expr.Candidate("brand_id")).Into("rating").Float().Default(0),
			op.Fetch("brand_user/score").Key(expr.Tuple(expr.Candidate("brand_id"), expr.Context("user_id"))).Into("brand_user_score").Float().Default(0),
		},
	}

	out, err := executor.Execute(
		context.Background(),
		node,
		runtime.NodeInput{
			State: runtime.State{
				Context: runtime.Context{"user_id": "u1"},
				Candidates: []runtime.Candidate{
					{"brand_id": 101},
					{"brand_id": 999},
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

	if got, want := out.Candidates[0]["rating"], 4.8; got != want {
		t.Fatalf("unexpected first rating: got %v want %v", got, want)
	}
	if got, want := store.Calls(), 3; got != want {
		t.Fatalf("unexpected feature store call count: got %d want %d", got, want)
	}
	if got := store.MaxInFlight(); got < 2 {
		t.Fatalf("expected parallel entity fetches, got max in-flight %d", got)
	}
	if got := store.MaxInFlight(); got > 2 {
		t.Fatalf("expected max in-flight <= 2, got %d", got)
	}
}

func TestCandidatesFetchExecutorFiltersNilCandidates(t *testing.T) {
	executor := CandidatesFetchExecutor{}
	store := testutil.NewCountingFeatureStore(featurestore.NewFileStore(filepath.Join("..", "..", "testdata", "featurestore", "unit")))
	node := pipeline.CompiledNode{
		ID:   "cand_fetch",
		Kind: pipeline.NodeKindCandidatesFetch,
		Ops: []op.Op{
			op.Fetch("user/segment").Key(expr.Context("user_id")).Into("user_segment").Float().Default(0),
			op.Fetch("brand/rating").Key(expr.Candidate("brand_id")).Into("rating").Float().Default(0),
		},
	}

	out, err := executor.Execute(
		context.Background(),
		node,
		runtime.NodeInput{
			State: runtime.State{
				Context: runtime.Context{"user_id": "u1"},
				Candidates: []runtime.Candidate{
					nil,
					{"brand_id": 101},
					nil,
					{"brand_id": 999},
				},
			},
		},
		&runtime.ExecEnv{
			FeatureStore:                    store,
			FeatureStoreFetchMaxParallelism: 1,
		},
	)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if got, want := len(out.Candidates), 2; got != want {
		t.Fatalf("unexpected candidate count after nil filtering: got %d want %d", got, want)
	}
	if got, want := out.Candidates[0]["rating"], 4.8; got != want {
		t.Fatalf("unexpected first rating: got %v want %v", got, want)
	}
	if got, want := out.Candidates[1]["rating"], 0.0; got != want {
		t.Fatalf("unexpected defaulted second rating: got %v want %v", got, want)
	}
	if got, want := out.Candidates[0]["user_segment"], 0.75; got != want {
		t.Fatalf("unexpected first user_segment: got %v want %v", got, want)
	}
	if got, want := out.Candidates[1]["user_segment"], 0.75; got != want {
		t.Fatalf("unexpected second user_segment: got %v want %v", got, want)
	}
	if got, want := store.Calls(), 2; got != want {
		t.Fatalf("unexpected feature store call count: got %d want %d", got, want)
	}
}

func TestCandidatesFetchExecutorDeduplicatesRepeatedSourceFeature(t *testing.T) {
	executor := CandidatesFetchExecutor{}
	store := testutil.NewCountingFeatureStore(featurestore.NewFileStore(filepath.Join("..", "..", "testdata", "featurestore", "unit")))
	node := pipeline.CompiledNode{
		ID:   "cand_fetch",
		Kind: pipeline.NodeKindCandidatesFetch,
		Ops: []op.Op{
			op.Fetch("brand/rating").Key(expr.Candidate("brand_id")).Into("rating").Float().Default(0),
			op.Fetch("brand/rating").Key(expr.Candidate("brand_id")).Into("rating_copy").Float().Default(0),
		},
	}

	out, err := executor.Execute(
		context.Background(),
		node,
		runtime.NodeInput{
			State: runtime.State{
				Candidates: []runtime.Candidate{
					{"brand_id": 101},
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

	if got, want := out.Candidates[0]["rating"], 4.8; got != want {
		t.Fatalf("unexpected rating: got %v want %v", got, want)
	}
	if got, want := out.Candidates[0]["rating_copy"], 4.8; got != want {
		t.Fatalf("unexpected rating_copy: got %v want %v", got, want)
	}
	if got, want := store.Calls(), 1; got != want {
		t.Fatalf("unexpected feature store call count: got %d want %d", got, want)
	}
	logs := store.Logs()
	if got, want := len(logs[0].Features), 1; got != want {
		t.Fatalf("unexpected feature count in request: got %d want %d", got, want)
	}
}

func TestCandidatesFetchExecutorRecordsFeatureFetchMetrics(t *testing.T) {
	executor := CandidatesFetchExecutor{}
	recorder := metrics.NewTestRecorder(nil)
	store := testutil.NewCountingFeatureStore(featurestore.NewFileStore(filepath.Join("..", "..", "testdata", "featurestore", "unit")))
	node := pipeline.CompiledNode{
		ID:        "cand_fetch",
		StageName: "cand_fetch",
		Kind:      pipeline.NodeKindCandidatesFetch,
		Ops: []op.Op{
			op.Fetch("brand/rating").Key(expr.Candidate("brand_id")).Into("rating").Float().Default(0),
			op.Fetch("user/segment").Key(expr.Context("user_id")).Into("segment").Float().Default(0),
		},
	}

	_, err := executor.Execute(
		context.Background(),
		node,
		runtime.NodeInput{
			State: runtime.State{
				Context: runtime.Context{"user_id": "u1"},
				Candidates: []runtime.Candidate{
					{"brand_id": 101},
				},
				Meta: runtime.Meta{Pipeline: "cand_metrics"},
			},
		},
		&runtime.ExecEnv{
			FeatureStore: store,
			Metrics:      recorder,
		},
	)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if got, want := len(recorder.FeatureFetches), 2; got != want {
		t.Fatalf("unexpected feature fetch metric count: got %d want %d", got, want)
	}
}

type fakeNodeFeatureStore struct {
	rows []map[string]any
	err  error
}

func (s fakeNodeFeatureStore) Fetch(ctx context.Context, features []string, keys ...runtime.Key) ([]map[string]any, error) {
	_ = ctx
	_ = features
	_ = keys
	return s.rows, s.err
}
