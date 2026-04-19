package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/dmitryBe/weaver/internal/adapters/featurestore"
	"github.com/dmitryBe/weaver/internal/dsl/expr"
	"github.com/dmitryBe/weaver/internal/dsl/expr/request"
	"github.com/dmitryBe/weaver/internal/dsl/expr/response"
	"github.com/dmitryBe/weaver/internal/dsl/input"
	"github.com/dmitryBe/weaver/internal/dsl/merge"
	"github.com/dmitryBe/weaver/internal/dsl/op"
	"github.com/dmitryBe/weaver/internal/dsl/param"
	"github.com/dmitryBe/weaver/internal/dsl/pipeline"
	"github.com/dmitryBe/weaver/internal/dsl/resilient"
	"github.com/dmitryBe/weaver/internal/dsl/retrieve"
	"github.com/dmitryBe/weaver/internal/dsl/types"
	"github.com/dmitryBe/weaver/internal/metrics"
	"github.com/dmitryBe/weaver/internal/nodes"
	"github.com/dmitryBe/weaver/internal/runtime"
	"github.com/dmitryBe/weaver/internal/testutil"
)

func TestEngineRunsTrendingWithMocks(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		switch r.URL.Path {
		case "/embed":
			writeJSON(t, w, map[string]any{
				"response": [][]float64{{0.1, 0.2, 0.3}},
			})
		case "/score":
			items, ok := payload["request"].([]any)
			if !ok {
				http.Error(w, fmt.Sprintf("expected request items, got %T", payload["request"]), http.StatusBadRequest)
				return
			}
			scores := make([]float64, 0, len(items))
			for _, raw := range items {
				item := raw.(map[string]any)
				rating, _ := item["rating"].(float64)
				priceTier := toFloat(item["price_tier"])
				brandUserScore, _ := item["brand_user_score"].(float64)
				scores = append(scores, rating+brandUserScore-(0.1*priceTier))
			}
			writeJSON(t, w, map[string]any{
				"scores": scores,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	// KV-only variant of Trending so Phase 4 can keep KG/Qdrant unimplemented.
	trendingKVOnly := pipeline.New("trending_kv_only").
		Input(
			input.String("city"),
			input.String("user_id"),
			input.String("vertical"),
		).
		Resilience(resilient.Standard()).
		Context(
			"ctx_fetch",
			op.Fetch("user/history_brands").
				Key(expr.Context("user_id")).
				Into("user_history_brand_ids"),
			op.Fetch("user/history_actions").
				Key(expr.Context("user_id")).
				Into("user_history_actions"),
			op.Fetch("user/segment").
				Key(expr.Context("user_id")).
				Into("user_segment").
				Float().
				Default(0),
		).
		Context(
			"ctx_embed",
			op.Model("embed_user").
				Endpoint(server.URL+"/embed").
				Request(
					request.Object(
						request.Field("user_id", expr.Context("user_id")),
						request.Field("history_brand_ids", expr.Context("user_history_brand_ids")),
						request.Field("history_actions", expr.Context("user_history_actions")),
					),
				).
				Response(response.Path("response.0")).
				Into("user_embedding"),
		).
		Context(
			"ctx_prepare",
			op.Set("u_segment", expr.Context("user_segment")).Float(),
			op.Set("city_copy", expr.Context("city")).String(),
			op.Set(
				"user_history_actions",
				expr.PadLeft(expr.Context("user_history_actions"), 10, 0),
			).AsType(types.Array(types.Int)),
		).
		Retrieve(
			"retrieve_candidates",
			retrieve.Parallel(
				retrieve.KV("trending/brands").
					Key(expr.Tuple(expr.Context("city"), expr.Context("vertical"))).
					TopK(100),
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
			op.Fetch("brand/price_tier").
				Key(expr.Candidate("brand_id")).
				Into("price_tier").
				Int().
				Default(0),
			op.Fetch("brand_user/score").
				Key(expr.Tuple(expr.Candidate("brand_id"), expr.Context("user_id"))).
				Into("brand_user_score").
				Float().
				Default(0),
		).
		Candidates(
			"cand_prepare",
			op.Set("user_id", expr.Context("user_id")).String(),
		).
		Candidates(
			"cand_score",
			op.Model("rank_brands").
				Endpoint(server.URL+"/score").
				Request(
					request.Object(
						request.Field("context.user_id", expr.Context("user_id")),
						request.Field(
							"request",
							request.ForEachCandidate(
								request.Object(
									request.Field("id", expr.Candidate("id")),
									request.Field("rating", expr.Candidate("rating")),
									request.Field("price_tier", expr.Candidate("price_tier")),
									request.Field("brand_user_score", expr.Candidate("brand_user_score")),
								),
							),
						),
					),
				).
				Response(response.Scores()).
				Into("score"),
		).
		Candidates(
			"post_rank",
			op.SortBy("score", op.Desc),
			op.Take(10),
		)

	compiled, err := pipeline.Compile(trendingKVOnly)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	registry := runtime.NewRegistry()
	nodes.RegisterDefaults(registry)

	engine := runtime.NewEngine(registry)
	store := testutil.NewCountingFeatureStore(featurestore.NewFileStore(filepath.Join("..", "..", "testdata", "featurestore", "unit")))
	env := &runtime.ExecEnv{
		FeatureStore:                    store,
		FeatureStoreFetchMaxParallelism: 2,
	}

	initial := runtime.State{
		Context: runtime.Context{
			"city":     "almaty",
			"user_id":  "u1",
			"vertical": "food",
		},
		Meta: runtime.Meta{
			RequestID: "req-1",
		},
	}

	final, err := engine.Run(context.Background(), compiled, initial, env)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}

	if got, want := final.Meta.Pipeline, "trending_kv_only"; got != want {
		t.Fatalf("unexpected pipeline name: got %q want %q", got, want)
	}
	if _, ok := final.Context["user_embedding"]; !ok {
		t.Fatal("expected user_embedding in final context")
	}

	actions, ok := final.Context["user_history_actions"].([]int)
	if !ok {
		t.Fatalf("expected padded user_history_actions to be []int, got %T", final.Context["user_history_actions"])
	}
	if got, want := len(actions), 10; got != want {
		t.Fatalf("unexpected padded actions length: got %d want %d", got, want)
	}

	if len(final.Candidates) == 0 {
		t.Fatal("expected final candidates")
	}
	if len(final.Candidates) > 10 {
		t.Fatalf("expected at most 10 candidates, got %d", len(final.Candidates))
	}

	seen := make(map[string]struct{}, len(final.Candidates))
	lastScore := 1e9
	for _, candidate := range final.Candidates {
		brandID := fmt.Sprint(candidate["brand_id"])
		if _, exists := seen[brandID]; exists {
			t.Fatalf("duplicate brand_id %q after merge dedup", brandID)
		}
		seen[brandID] = struct{}{}

		if _, ok := candidate["user_id"]; !ok {
			t.Fatal("expected candidate user_id to be copied from context")
		}
		score, ok := candidate["score"].(float64)
		if !ok {
			t.Fatalf("expected candidate score to be float64, got %T", candidate["score"])
		}
		if score > lastScore {
			t.Fatalf("candidates not sorted descending by score: %v then %v", lastScore, score)
		}
		lastScore = score
	}
	if got, want := store.Calls(), 4; got != want {
		// ctx_fetch (user/*) + retrieve (trending/*) + cand_fetch (user/* + brand/* + brand_user/*)
		t.Fatalf("unexpected feature store call count: got %d want %d", got, want)
	}
}

func TestEngineRunsRetrieveStageWithKVAndKGMerge(t *testing.T) {
	queryDir := t.TempDir()
	queryPath := filepath.Join(queryDir, "popular_brands.cypher")
	if err := os.WriteFile(queryPath, []byte("RETURN $city AS city"), 0o600); err != nil {
		t.Fatalf("write query file: %v", err)
	}

	spec := pipeline.New("retrieve_merge_kv_kg").
		Input(
			input.String("city"),
			input.String("vertical"),
		).
		Retrieve(
			"retrieve_candidates",
			retrieve.Parallel(
				retrieve.KV("trending/brands").
					Key(expr.Tuple(expr.Context("city"), expr.Context("vertical"))).
					TopK(2),
				retrieve.KG(queryPath).
					Params(param.String("city", expr.Context("city"))).
					TopK(2),
			).Merge(
				merge.Default().DedupBy("brand_id").SortByScore("score"),
			),
		)

	compiled, err := pipeline.Compile(spec)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	registry := runtime.NewRegistry()
	nodes.RegisterDefaults(registry)

	engine := runtime.NewEngine(registry)
	store := featurestore.NewFileStore(filepath.Join("..", "..", "testdata", "featurestore", "unit"))
	env := &runtime.ExecEnv{
		FeatureStore: store,
		KnowledgeGraphs: map[string]runtime.KnowledgeGraph{
			"main": &fakeEngineKnowledgeGraph{
				rows: []map[string]any{
					{"brand_id": 101, "score": 0.95, "brand_name": "Brand 101 from KG"},
					{"brand_id": 999, "score": 0.75, "brand_name": "Brand 999 from KG"},
				},
			},
		},
	}

	final, err := engine.Run(context.Background(), compiled, runtime.State{
		Context: runtime.Context{
			"city":     "almaty",
			"vertical": "food",
		},
	}, env)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}

	if got, want := len(final.Candidates), 3; got != want {
		t.Fatalf("unexpected candidate count: got %d want %d", got, want)
	}
	if got := fmt.Sprint(final.Candidates[0]["brand_id"]); got != "101" {
		t.Fatalf("expected top candidate brand_id 101, got %s", got)
	}
	if got := fmt.Sprint(final.Candidates[1]["brand_id"]); got != "102" {
		t.Fatalf("expected second candidate brand_id 102, got %s", got)
	}
	if got := fmt.Sprint(final.Candidates[2]["brand_id"]); got != "999" {
		t.Fatalf("expected third candidate brand_id 999, got %s", got)
	}
}

func TestEngineDebugSnapshotsRemainIsolatedAcrossLaterNestedMutations(t *testing.T) {
	plan := &pipeline.CompiledPlan{
		SpecName: "debug_snapshots",
		Nodes: []pipeline.CompiledNode{
			{
				ID:        "ctx_seed",
				StageName: "ctx_seed",
				Kind:      pipeline.NodeKindContextMutate,
			},
			{
				ID:        "ctx_mutate_nested",
				StageName: "ctx_mutate_nested",
				Kind:      pipeline.NodeKindContextMutate,
				DependsOn: []string{"ctx_seed"},
			},
			{
				ID:        "retrieve_candidates:merge",
				StageName: "retrieve_candidates",
				Kind:      pipeline.NodeKindRetrieveMerge,
				DependsOn: []string{"ctx_mutate_nested"},
			},
			{
				ID:        "post_rank",
				StageName: "post_rank",
				Kind:      pipeline.NodeKindCandidatesMutate,
				DependsOn: []string{"retrieve_candidates:merge"},
			},
		},
		Groups: []pipeline.ExecutionGroup{
			{Index: 0, NodeIDs: []string{"ctx_seed"}},
			{Index: 1, NodeIDs: []string{"ctx_mutate_nested"}},
			{Index: 2, NodeIDs: []string{"retrieve_candidates:merge"}},
			{Index: 3, NodeIDs: []string{"post_rank"}},
		},
		FinalNode: "post_rank",
	}

	registry := runtime.NewRegistry()
	registry.Register(pipeline.NodeKindContextMutate, debugMutationExecutor{})
	registry.Register(pipeline.NodeKindRetrieveMerge, debugMutationExecutor{})
	registry.Register(pipeline.NodeKindCandidatesMutate, debugMutationExecutor{})

	engine := runtime.NewEngine(registry)
	result, err := engine.RunWithOptions(context.Background(), plan, runtime.State{
		Meta: runtime.Meta{RequestID: "req-debug"},
	}, &runtime.ExecEnv{}, runtime.RunOptions{Debug: true})
	if err != nil {
		t.Fatalf("run with debug failed: %v", err)
	}
	if result.DebugInfo == nil {
		t.Fatal("expected debug info")
	}
	if got, want := len(result.DebugInfo.Steps), 4; got != want {
		t.Fatalf("unexpected debug step count: got %d want %d", got, want)
	}

	first := result.DebugInfo.Steps[0]
	if got, want := first.StageName, "ctx_seed"; got != want {
		t.Fatalf("unexpected first stage: got %q want %q", got, want)
	}
	payload, ok := first.Context["payload"].(map[string]any)
	if !ok {
		t.Fatalf("expected payload map in first snapshot, got %T", first.Context["payload"])
	}
	values, ok := payload["values"].([]any)
	if !ok {
		t.Fatalf("expected payload values slice, got %T", payload["values"])
	}
	if got, want := values[0], "initial"; got != want {
		t.Fatalf("first snapshot mutated unexpectedly: got %#v want %#v", got, want)
	}

	second := result.DebugInfo.Steps[1]
	payload, ok = second.Context["payload"].(map[string]any)
	if !ok {
		t.Fatalf("expected payload map in second snapshot, got %T", second.Context["payload"])
	}
	values, ok = payload["values"].([]any)
	if !ok {
		t.Fatalf("expected payload values slice in second snapshot, got %T", payload["values"])
	}
	if got, want := values[0], "mutated"; got != want {
		t.Fatalf("unexpected second snapshot value: got %#v want %#v", got, want)
	}

	third := result.DebugInfo.Steps[2]
	if got, want := third.Scope, runtime.DebugScopeCandidates; got != want {
		t.Fatalf("unexpected retrieve scope: got %q want %q", got, want)
	}
	if len(third.Candidates) != 1 {
		t.Fatalf("expected retrieve snapshot candidates, got %d", len(third.Candidates))
	}
	if _, ok := third.Candidates[0]["rank"]; ok {
		t.Fatalf("retrieve snapshot should not include later candidate fields: %#v", third.Candidates[0])
	}

	fourth := result.DebugInfo.Steps[3]
	if got, want := fourth.NodeID, "post_rank"; got != want {
		t.Fatalf("unexpected final node id: got %q want %q", got, want)
	}
	if got, want := fourth.Candidates[0]["rank"], 1; got != want {
		t.Fatalf("unexpected final candidate rank: got %#v want %#v", got, want)
	}
}

type debugMutationExecutor struct{}

func (debugMutationExecutor) Execute(_ context.Context, node pipeline.CompiledNode, in runtime.NodeInput, _ *runtime.ExecEnv) (runtime.NodeOutput, error) {
	switch node.ID {
	case "ctx_seed":
		return runtime.NodeOutput{
			Context: runtime.Context{
				"payload": map[string]any{
					"values": []any{"initial"},
				},
			},
		}, nil
	case "ctx_mutate_nested":
		out := runtime.CloneContext(in.State.Context)
		payload := out["payload"].(map[string]any)
		values := payload["values"].([]any)
		values[0] = "mutated"
		payload["values"] = values
		out["payload"] = payload
		return runtime.NodeOutput{Context: out}, nil
	case "retrieve_candidates:merge":
		return runtime.NodeOutput{
			Candidates: []runtime.Candidate{
				{
					"brand_id": 101,
					"payload": map[string]any{
						"score_parts": []any{"base"},
					},
				},
			},
		}, nil
	case "post_rank":
		out := runtime.CloneCandidates(in.State.Candidates)
		payload := out[0]["payload"].(map[string]any)
		scoreParts := payload["score_parts"].([]any)
		scoreParts[0] = "rescored"
		payload["score_parts"] = scoreParts
		out[0]["payload"] = payload
		out[0]["rank"] = 1
		return runtime.NodeOutput{Candidates: out}, nil
	default:
		return runtime.NodeOutput{}, fmt.Errorf("unexpected node id %q", node.ID)
	}
}

func TestEngineRecordsNodeAndStageMetrics(t *testing.T) {
	spec := pipeline.New("retrieve_metrics").
		Input(
			input.String("city"),
			input.String("vertical"),
		).
		Retrieve(
			"retrieve_candidates",
			retrieve.Parallel(
				retrieve.KV("trending/brands").
					Key(expr.Tuple(expr.Context("city"), expr.Context("vertical"))).
					TopK(2),
				retrieve.KV("trending/brands").
					Key(expr.Tuple(expr.Context("city"), expr.Context("vertical"))).
					TopK(2),
			).Merge(
				merge.Default().DedupBy("brand_id").SortByScore("score"),
			),
		)

	compiled, err := pipeline.Compile(spec)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	recorder := metrics.NewTestRecorder(nil)
	registry := runtime.NewRegistry()
	nodes.RegisterDefaults(registry)

	engine := runtime.NewEngine(registry)
	env := &runtime.ExecEnv{
		FeatureStore: featurestore.NewFileStore(filepath.Join("..", "..", "testdata", "featurestore", "unit")),
		Metrics:      recorder,
	}

	_, err = engine.Run(context.Background(), compiled, runtime.State{
		Context: runtime.Context{
			"city":     "almaty",
			"vertical": "food",
		},
		Meta: runtime.Meta{
			Pipeline: "retrieve_metrics",
		},
	}, env)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}

	if got, want := len(recorder.Nodes), 3; got != want {
		t.Fatalf("unexpected node metric count: got %d want %d", got, want)
	}
	if got, want := len(recorder.Stages), 1; got != want {
		t.Fatalf("unexpected stage metric count: got %d want %d", got, want)
	}
	if got, want := recorder.Stages[0].StageCategory, metrics.StageCategoryRetrieveCandidates; got != want {
		t.Fatalf("unexpected stage category: got %q want %q", got, want)
	}
}

func TestEngineAppliesResiliencePerIONodeIndependently(t *testing.T) {
	spec := pipeline.New("resilience_independent").
		Input(input.String("user_id")).
		Resilience(resilient.Default(200*time.Millisecond, 1)).
		Context("io_one", op.Fetch("user/segment").Key(expr.Context("user_id")).Into("segment")).
		Context("io_two", op.Fetch("user/segment").Key(expr.Context("user_id")).Into("segment_copy"))

	compiled, err := pipeline.Compile(spec)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	registry := runtime.NewRegistry()
	executor := &scriptedExecutor{
		scripts: map[string][]scriptedOutcome{
			"io_one": {
				{err: errors.New("temporary failure")},
				{out: runtime.NodeOutput{Context: runtime.Context{"segment": "a"}}},
			},
			"io_two": {
				{err: errors.New("temporary failure")},
				{out: runtime.NodeOutput{Context: runtime.Context{"segment": "a", "segment_copy": "b"}}},
			},
		},
	}
	registry.Register(pipeline.NodeKindContextFetch, executor)

	recorder := metrics.NewTestRecorder(nil)
	engine := runtime.NewEngine(registry)
	_, err = engine.Run(context.Background(), compiled, runtime.State{
		Context: runtime.Context{"user_id": "u1"},
		Meta:    runtime.Meta{Pipeline: "resilience_independent"},
	}, &runtime.ExecEnv{Metrics: recorder})
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}

	if got, want := executor.Attempts("io_one"), 2; got != want {
		t.Fatalf("unexpected attempts for io_one: got %d want %d", got, want)
	}
	if got, want := executor.Attempts("io_two"), 2; got != want {
		t.Fatalf("unexpected attempts for io_two: got %d want %d", got, want)
	}
	if got, want := len(recorder.Resilience), 2; got != want {
		t.Fatalf("unexpected resilience metric count: got %d want %d", got, want)
	}
	for _, metric := range recorder.Resilience {
		if got, want := metric.Status, metrics.StatusOK; got != want {
			t.Fatalf("unexpected resilience status: got %q want %q", got, want)
		}
		if got, want := metric.Reason, metrics.ReasonRetriedSuccess; got != want {
			t.Fatalf("unexpected resilience reason: got %q want %q", got, want)
		}
		if got, want := metric.Attempts, 2; got != want {
			t.Fatalf("unexpected resilience attempts: got %d want %d", got, want)
		}
	}
}

func TestEngineRecordsTimeoutWhenResilienceBudgetReached(t *testing.T) {
	spec := pipeline.New("resilience_timeout").
		Input(input.String("user_id")).
		Resilience(resilient.Default(20*time.Millisecond, 2)).
		Context("io_timeout", op.Fetch("user/segment").Key(expr.Context("user_id")).Into("segment"))

	compiled, err := pipeline.Compile(spec)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	registry := runtime.NewRegistry()
	registry.Register(pipeline.NodeKindContextFetch, &scriptedExecutor{
		scripts: map[string][]scriptedOutcome{
			"io_timeout": {
				{sleep: 50 * time.Millisecond, err: context.DeadlineExceeded},
			},
		},
	})

	recorder := metrics.NewTestRecorder(nil)
	engine := runtime.NewEngine(registry)
	_, err = engine.Run(context.Background(), compiled, runtime.State{
		Context: runtime.Context{"user_id": "u1"},
		Meta:    runtime.Meta{Pipeline: "resilience_timeout"},
	}, &runtime.ExecEnv{Metrics: recorder})
	if err == nil {
		t.Fatal("expected timeout error")
	}

	if got, want := len(recorder.Resilience), 1; got != want {
		t.Fatalf("unexpected resilience metric count: got %d want %d", got, want)
	}
	metric := recorder.Resilience[0]
	if got, want := metric.Status, metrics.StatusError; got != want {
		t.Fatalf("unexpected resilience status: got %q want %q", got, want)
	}
	if got, want := metric.Reason, metrics.ReasonTimeout; got != want {
		t.Fatalf("unexpected resilience reason: got %q want %q", got, want)
	}
	if got, want := metric.Attempts, 1; got != want {
		t.Fatalf("unexpected resilience attempts: got %d want %d", got, want)
	}
}

func toFloat(value any) float64 {
	switch current := value.(type) {
	case int:
		return float64(current)
	case float64:
		return current
	default:
		return 0
	}
}

func writeJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}

type fakeEngineKnowledgeGraph struct {
	rows []map[string]any
}

func (g *fakeEngineKnowledgeGraph) Query(ctx context.Context, query string, params map[string]any) ([]map[string]any, error) {
	_ = ctx
	_ = query
	_ = params
	return g.rows, nil
}

func (*fakeEngineKnowledgeGraph) Close(ctx context.Context) error {
	_ = ctx
	return nil
}

type scriptedOutcome struct {
	sleep time.Duration
	out   runtime.NodeOutput
	err   error
}

type scriptedExecutor struct {
	mu       sync.Mutex
	scripts  map[string][]scriptedOutcome
	attempts map[string]int
}

func (e *scriptedExecutor) Execute(ctx context.Context, node pipeline.CompiledNode, in runtime.NodeInput, env *runtime.ExecEnv) (runtime.NodeOutput, error) {
	_ = in
	_ = env

	e.mu.Lock()
	if e.attempts == nil {
		e.attempts = make(map[string]int)
	}
	index := e.attempts[node.StageName]
	e.attempts[node.StageName] = index + 1
	outcome := scriptedOutcome{}
	if scripts, ok := e.scripts[node.StageName]; ok && index < len(scripts) {
		outcome = scripts[index]
	}
	e.mu.Unlock()

	if outcome.sleep > 0 {
		timer := time.NewTimer(outcome.sleep)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return runtime.NodeOutput{}, ctx.Err()
		case <-timer.C:
		}
	}
	if outcome.err != nil {
		return runtime.NodeOutput{}, outcome.err
	}
	return outcome.out, nil
}

func (e *scriptedExecutor) Attempts(stage string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.attempts[stage]
}
