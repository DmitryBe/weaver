package api_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	cacheadapter "github.com/dmitryBe/weaver/internal/adapters/cache"
	"github.com/dmitryBe/weaver/internal/adapters/featurestore"
	"github.com/dmitryBe/weaver/internal/api"
	dslcache "github.com/dmitryBe/weaver/internal/dsl/cache"
	"github.com/dmitryBe/weaver/internal/dsl/expr"
	"github.com/dmitryBe/weaver/internal/dsl/expr/request"
	"github.com/dmitryBe/weaver/internal/dsl/expr/response"
	"github.com/dmitryBe/weaver/internal/dsl/input"
	"github.com/dmitryBe/weaver/internal/dsl/merge"
	"github.com/dmitryBe/weaver/internal/dsl/op"
	"github.com/dmitryBe/weaver/internal/dsl/pipeline"
	"github.com/dmitryBe/weaver/internal/dsl/resilient"
	"github.com/dmitryBe/weaver/internal/dsl/retrieve"
	"github.com/dmitryBe/weaver/internal/nodes"
	"github.com/dmitryBe/weaver/internal/runner"
	"github.com/dmitryBe/weaver/internal/runtime"
	"github.com/dmitryBe/weaver/internal/testutil"
	_ "github.com/dmitryBe/weaver/pipelines"
)

func TestServeSurfaceExecutesPipeline(t *testing.T) {
	registry := pipeline.NewRegistry()
	registry.Register("test_suite", "trending_api_test", func() *pipeline.Spec {
		return pipeline.New().
			Input(
				input.String("city"),
				input.String("vertical"),
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
				op.SortBy("score", op.Desc),
				op.Take(2),
			)
	})
	if err := registry.BuildAll(); err != nil {
		t.Fatalf("BuildAll failed: %v", err)
	}

	server := api.NewServer(newTestRunner(registry, &runtime.ExecEnv{
		FeatureStore: featurestore.NewFileStore(filepath.Join("..", "..", "testdata", "featurestore", "unit")),
	}))

	body, err := json.Marshal(api.SurfaceRequest{
		RequestID: "req-123",
		Context: map[string]any{
			"city":     "almaty",
			"vertical": "food",
		},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/surface/test_suite.trending_api_test", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d body=%s", rec.Code, rec.Body.String())
	}

	var resp api.SurfaceResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if resp.RequestID != "req-123" {
		t.Fatalf("unexpected request id: got %q", resp.RequestID)
	}
	if resp.Pipeline != "test_suite.trending_api_test" {
		t.Fatalf("unexpected pipeline: got %q", resp.Pipeline)
	}
	if len(resp.Candidates) != 2 {
		t.Fatalf("unexpected candidate count: got %d", len(resp.Candidates))
	}
	if resp.CacheHit {
		t.Fatal("expected first response to miss cache")
	}
	if got := resp.Candidates[0]["brand_id"]; got != float64(101) {
		t.Fatalf("unexpected first candidate brand_id: got %#v", got)
	}
	if _, ok := resp.Candidates[0]["rating"]; !ok {
		t.Fatalf("expected fetched rating on candidate: %#v", resp.Candidates[0])
	}
}

func TestServeSurfaceReturnsDebugSnapshots(t *testing.T) {
	registry := pipeline.NewRegistry()
	registry.Register("test_suite", "trending_api_debug", func() *pipeline.Spec {
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

	server := api.NewServer(newTestRunner(registry, &runtime.ExecEnv{
		FeatureStore: featurestore.NewFileStore(filepath.Join("..", "..", "testdata", "featurestore", "unit")),
	}))

	body, err := json.Marshal(api.SurfaceRequest{
		RequestID: "req-debug",
		Context: map[string]any{
			"city":     "almaty",
			"vertical": "food",
		},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/surface/test_suite.trending_api_debug?debug=true", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d body=%s", rec.Code, rec.Body.String())
	}

	var resp api.SurfaceResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.DebugInfo == nil {
		t.Fatal("expected debug info")
	}
	if got, want := len(resp.DebugInfo.Steps), 4; got != want {
		t.Fatalf("unexpected debug step count: got %d want %d", got, want)
	}
	if got, want := resp.DebugInfo.Steps[0].StageName, "ctx_prepare"; got != want {
		t.Fatalf("unexpected first stage: got %q want %q", got, want)
	}
	if got, want := resp.DebugInfo.Steps[1].NodeID, "retrieve_candidates:merge"; got != want {
		t.Fatalf("unexpected retrieve node id: got %q want %q", got, want)
	}
	if _, ok := resp.DebugInfo.Steps[1].Candidates[0]["rating"]; ok {
		t.Fatalf("retrieve snapshot should not include rating yet: %#v", resp.DebugInfo.Steps[1].Candidates[0])
	}
	if _, ok := resp.DebugInfo.Steps[2].Candidates[0]["rating"]; !ok {
		t.Fatalf("candidate fetch snapshot should include rating: %#v", resp.DebugInfo.Steps[2].Candidates[0])
	}
}

func TestServeSurfaceReportsCacheHit(t *testing.T) {
	registry := pipeline.NewRegistry()
	registry.Register("test_suite", "trending_api_cache_test", func() *pipeline.Spec {
		return pipeline.New().
			Input(
				input.String("city"),
				input.String("vertical"),
			).
			Cache(
				dslcache.Key(expr.Tuple(expr.Context("city"), expr.Context("vertical"))),
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
				op.SortBy("score", op.Desc),
				op.Take(2),
			)
	})
	if err := registry.BuildAll(); err != nil {
		t.Fatalf("BuildAll failed: %v", err)
	}

	store := testutil.NewCountingFeatureStore(featurestore.NewFileStore(filepath.Join("..", "..", "testdata", "featurestore", "unit")))
	server := api.NewServer(newTestRunner(registry, &runtime.ExecEnv{
		FeatureStore:    store,
		Cache:           cacheadapter.NewInMemoryLRU(64),
		CacheDefaultTTL: 5 * time.Minute,
	}))

	body, err := json.Marshal(api.SurfaceRequest{
		RequestID: "req-123",
		Context: map[string]any{
			"city":     "almaty",
			"vertical": "food",
		},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	firstReq := httptest.NewRequest(http.MethodPost, "/v1/surface/test_suite.trending_api_cache_test", bytes.NewReader(body))
	firstRec := httptest.NewRecorder()
	server.ServeHTTP(firstRec, firstReq)
	if firstRec.Code != http.StatusOK {
		t.Fatalf("unexpected first status: got %d body=%s", firstRec.Code, firstRec.Body.String())
	}

	secondReq := httptest.NewRequest(http.MethodPost, "/v1/surface/test_suite.trending_api_cache_test", bytes.NewReader(body))
	secondRec := httptest.NewRecorder()
	server.ServeHTTP(secondRec, secondReq)
	if secondRec.Code != http.StatusOK {
		t.Fatalf("unexpected second status: got %d body=%s", secondRec.Code, secondRec.Body.String())
	}

	var secondResp api.SurfaceResponse
	if err := json.NewDecoder(secondRec.Body).Decode(&secondResp); err != nil {
		t.Fatalf("decode second response: %v", err)
	}
	if !secondResp.CacheHit {
		t.Fatal("expected second response to hit cache")
	}
	if got, want := store.Calls(), 2; got != want {
		t.Fatalf("expected cached response to skip second pipeline execution: got %d want %d", got, want)
	}
}

func TestServeSurfaceBypassesCacheInDebugMode(t *testing.T) {
	registry := pipeline.NewRegistry()
	registry.Register("test_suite", "trending_api_cache_debug", func() *pipeline.Spec {
		return pipeline.New().
			Input(
				input.String("city"),
				input.String("vertical"),
			).
			Cache(
				dslcache.Key(expr.Tuple(expr.Context("city"), expr.Context("vertical"))),
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
			)
	})
	if err := registry.BuildAll(); err != nil {
		t.Fatalf("BuildAll failed: %v", err)
	}

	store := testutil.NewCountingFeatureStore(featurestore.NewFileStore(filepath.Join("..", "..", "testdata", "featurestore", "unit")))
	server := api.NewServer(newTestRunner(registry, &runtime.ExecEnv{
		FeatureStore:    store,
		Cache:           cacheadapter.NewInMemoryLRU(64),
		CacheDefaultTTL: 5 * time.Minute,
	}))

	body, err := json.Marshal(api.SurfaceRequest{
		RequestID: "req-debug-cache",
		Context: map[string]any{
			"city":     "almaty",
			"vertical": "food",
		},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	firstReq := httptest.NewRequest(http.MethodPost, "/v1/surface/test_suite.trending_api_cache_debug", bytes.NewReader(body))
	firstRec := httptest.NewRecorder()
	server.ServeHTTP(firstRec, firstReq)
	if firstRec.Code != http.StatusOK {
		t.Fatalf("unexpected first status: got %d body=%s", firstRec.Code, firstRec.Body.String())
	}

	debugReq := httptest.NewRequest(http.MethodPost, "/v1/surface/test_suite.trending_api_cache_debug?debug=true", bytes.NewReader(body))
	debugRec := httptest.NewRecorder()
	server.ServeHTTP(debugRec, debugReq)
	if debugRec.Code != http.StatusOK {
		t.Fatalf("unexpected debug status: got %d body=%s", debugRec.Code, debugRec.Body.String())
	}

	var resp api.SurfaceResponse
	if err := json.NewDecoder(debugRec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode debug response: %v", err)
	}
	if resp.CacheHit {
		t.Fatal("expected debug response to bypass cache")
	}
	if resp.DebugInfo == nil {
		t.Fatal("expected debug info on debug response")
	}
	if got, want := store.Calls(), 4; got != want {
		t.Fatalf("expected debug request to execute pipeline instead of cache hit: got %d want %d", got, want)
	}
}

func TestServeSurfaceRejectsInvalidInput(t *testing.T) {
	registry := pipeline.NewRegistry()
	registry.Register("test_suite", "input_validation_test", func() *pipeline.Spec {
		return pipeline.New().
			Input(input.String("city")).
			Candidates(
				"post_rank",
				op.Take(1),
			)
	})
	if err := registry.BuildAll(); err != nil {
		t.Fatalf("BuildAll failed: %v", err)
	}

	server := api.NewServer(newTestRunner(registry, &runtime.ExecEnv{}))

	body, err := json.Marshal(api.SurfaceRequest{
		RequestID: "req-bad",
		Context:   map[string]any{},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/surface/test_suite.input_validation_test", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: got %d body=%s", rec.Code, rec.Body.String())
	}

	var resp api.ErrorResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got, want := resp.Code, "INVALID_ARGUMENT"; got != want {
		t.Fatalf("unexpected error code: got %q want %q", got, want)
	}
	if got, want := resp.Category, "invalid_argument"; got != want {
		t.Fatalf("unexpected category: got %q want %q", got, want)
	}
}

func TestServeSurfaceAcceptsCandidateInputSchema(t *testing.T) {
	registry := pipeline.NewRegistry()
	registry.Register("test_suite", "candidate_input_validation_test", func() *pipeline.Spec {
		return pipeline.New().
			Input(
				input.String("user_id"),
				input.Candidates(
					input.String("id"),
					input.String("name"),
				),
			).
			Candidates(
				"post_rank",
				op.Take(1),
			)
	})
	if err := registry.BuildAll(); err != nil {
		t.Fatalf("BuildAll failed: %v", err)
	}

	server := api.NewServer(newTestRunner(registry, &runtime.ExecEnv{}))

	body, err := json.Marshal(api.SurfaceRequest{
		RequestID: "req-good",
		Context: map[string]any{
			"user_id": "u1",
		},
		Candidates: []map[string]any{
			{
				"id":   "1",
				"name": "Cafe",
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/surface/test_suite.candidate_input_validation_test", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestServeRegistryListsBuiltPipelines(t *testing.T) {
	registry := pipeline.NewRegistry()
	registry.Register("zeta", "one", func() *pipeline.Spec {
		return pipeline.New().Candidates("post_rank", op.Take(1))
	})
	registry.Register("alpha", "two", func() *pipeline.Spec {
		return pipeline.New().Candidates("post_rank", op.Take(1))
	})
	if err := registry.BuildAll(); err != nil {
		t.Fatalf("BuildAll failed: %v", err)
	}

	server := api.NewServer(newTestRunner(registry, &runtime.ExecEnv{}))
	req := httptest.NewRequest(http.MethodGet, "/v1/registry", nil)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d body=%s", rec.Code, rec.Body.String())
	}

	var resp api.RegistryResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if got, want := len(resp.Pipelines), 2; got != want {
		t.Fatalf("unexpected pipeline count: got %d want %d", got, want)
	}
	if got, want := resp.Pipelines[0].FullName, "alpha.two"; got != want {
		t.Fatalf("unexpected first full name: got %q want %q", got, want)
	}
	if got, want := resp.Pipelines[0].Domain, "alpha"; got != want {
		t.Fatalf("unexpected first domain: got %q want %q", got, want)
	}
	if got, want := resp.Pipelines[0].Name, "two"; got != want {
		t.Fatalf("unexpected first name: got %q want %q", got, want)
	}
}

func TestServeSurfaceReturnsNotFoundForUnknownPipeline(t *testing.T) {
	registry := pipeline.NewRegistry()
	registry.Register("test_suite", "known", func() *pipeline.Spec {
		return pipeline.New().Candidates("post_rank", op.Take(1))
	})
	if err := registry.BuildAll(); err != nil {
		t.Fatalf("BuildAll failed: %v", err)
	}

	server := api.NewServer(newTestRunner(registry, &runtime.ExecEnv{}))
	req := httptest.NewRequest(http.MethodPost, "/v1/surface/test_suite.missing", bytes.NewReader([]byte(`{"context":{}}`)))
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("unexpected status: got %d body=%s", rec.Code, rec.Body.String())
	}

	var resp api.ErrorResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got, want := resp.Code, "NOT_FOUND"; got != want {
		t.Fatalf("unexpected code: got %q want %q", got, want)
	}
	if got, want := resp.Category, "not_found"; got != want {
		t.Fatalf("unexpected category: got %q want %q", got, want)
	}
}

func TestServeSurfaceReturnsInvalidArgumentForMissingCandidateField(t *testing.T) {
	registry := pipeline.NewRegistry()
	registry.Register("test_suite", "score_candidates_input_error", func() *pipeline.Spec {
		return pipeline.New().
			Input(input.String("user_id")).
			Candidates(
				"cand_score",
				op.Model("rank_brands").
					Endpoint("http://example.invalid/score").
					Request(
						request.Object(
							request.Field("context.user_id", expr.Context("user_id")),
							request.Field(
								"candidates",
								request.ForEachCandidate(
									request.Object(
										request.Field("brand_id", expr.Candidate("brand_id")),
										request.Field("rating", expr.Candidate("rating")),
									),
								),
							),
						),
					).
					Response(response.Scores()).
					Into("score"),
			)
	})
	if err := registry.BuildAll(); err != nil {
		t.Fatalf("BuildAll failed: %v", err)
	}

	server := api.NewServer(newTestRunner(registry, &runtime.ExecEnv{}))
	body, err := json.Marshal(api.SurfaceRequest{
		RequestID: "req-candidate-bad",
		Context:   map[string]any{"user_id": "u1"},
		Candidates: []map[string]any{
			{"brand_id": 101},
		},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/surface/test_suite.score_candidates_input_error", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: got %d body=%s", rec.Code, rec.Body.String())
	}
	assertErrorResponse(t, rec, "INVALID_ARGUMENT", "invalid_argument", "invalid request candidates: missing field \"rating\"")
}

func TestServeSurfaceReturnsEmptyCandidatesForMissingRetrieveData(t *testing.T) {
	registry := pipeline.NewRegistry()
	registry.Register("test_suite", "retrieve_missing_data", func() *pipeline.Spec {
		return pipeline.New().
			Input(
				input.String("city"),
				input.String("vertical"),
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
			)
	})
	if err := registry.BuildAll(); err != nil {
		t.Fatalf("BuildAll failed: %v", err)
	}

	server := api.NewServer(newTestRunner(registry, &runtime.ExecEnv{
		FeatureStore: featurestore.NewFileStore(filepath.Join("..", "..", "testdata", "featurestore", "unit")),
	}))
	body, err := json.Marshal(api.SurfaceRequest{
		RequestID: "req-missing-data",
		Context: map[string]any{
			"city":     "dubai",
			"vertical": "food",
		},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/surface/test_suite.retrieve_missing_data", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d body=%s", rec.Code, rec.Body.String())
	}

	var resp api.SurfaceResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got, want := len(resp.Candidates), 0; got != want {
		t.Fatalf("unexpected candidate count: got %d want %d", got, want)
	}
}

func TestServeSurfaceReturnsDependencyFailedForUnavailableEndpoint(t *testing.T) {
	registry := pipeline.NewRegistry()
	registry.Register("test_suite", "dependency_failed", func() *pipeline.Spec {
		return pipeline.New().
			Input(input.String("user_id")).
			Context(
				"ctx_embed",
				op.Model("embed_user").
					Endpoint("http://127.0.0.1:1/embed").
					Request(
						request.Object(
							request.Field("user_id", expr.Context("user_id")),
						),
					).
					Response(response.Path("embedding")).
					Into("embedding"),
			)
	})
	if err := registry.BuildAll(); err != nil {
		t.Fatalf("BuildAll failed: %v", err)
	}

	server := api.NewServer(newTestRunner(registry, &runtime.ExecEnv{}))
	body, err := json.Marshal(api.SurfaceRequest{
		RequestID: "req-dependency-failed",
		Context:   map[string]any{"user_id": "u1"},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/surface/test_suite.dependency_failed", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("unexpected status: got %d body=%s", rec.Code, rec.Body.String())
	}
	assertErrorResponse(t, rec, "DEPENDENCY_FAILED", "dependency_failed", "upstream model endpoint unavailable")
}

func TestServeSurfaceReturnsDependencyTimeoutForModelTimeout(t *testing.T) {
	slowServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"embedding": []float64{0.1, 0.2}})
	}))
	defer slowServer.Close()

	registry := pipeline.NewRegistry()
	registry.Register("test_suite", "dependency_timeout", func() *pipeline.Spec {
		return pipeline.New().
			Input(input.String("user_id")).
			Resilience(resilient.Default(20*time.Millisecond, 0)).
			Context(
				"ctx_embed",
				op.Model("embed_user").
					Endpoint(slowServer.URL).
					Request(
						request.Object(
							request.Field("user_id", expr.Context("user_id")),
						),
					).
					Response(response.Path("embedding")).
					Into("embedding"),
			)
	})
	if err := registry.BuildAll(); err != nil {
		t.Fatalf("BuildAll failed: %v", err)
	}

	server := api.NewServer(newTestRunner(registry, &runtime.ExecEnv{}))
	body, err := json.Marshal(api.SurfaceRequest{
		RequestID: "req-timeout",
		Context:   map[string]any{"user_id": "u1"},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/surface/test_suite.dependency_timeout", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("unexpected status: got %d body=%s", rec.Code, rec.Body.String())
	}
	assertErrorResponse(t, rec, "DEPENDENCY_TIMEOUT", "dependency_timeout", "upstream model request timed out")
}

func TestServeSurfaceReturnsPipelineMisconfiguredForResponseMismatch(t *testing.T) {
	modelServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"response": []map[string]any{
				{"embedding": []float64{0.1, 0.2}},
			},
		})
	}))
	defer modelServer.Close()

	registry := pipeline.NewRegistry()
	registry.Register("test_suite", "response_mismatch", func() *pipeline.Spec {
		return pipeline.New().
			Input(input.String("user_id")).
			Context(
				"ctx_embed",
				op.Model("embed_user").
					Endpoint(modelServer.URL).
					Request(
						request.Object(
							request.Field("user_id", expr.Context("user_id")),
						),
					).
					Response(response.Path("embedding")).
					Into("embedding"),
			)
	})
	if err := registry.BuildAll(); err != nil {
		t.Fatalf("BuildAll failed: %v", err)
	}

	server := api.NewServer(newTestRunner(registry, &runtime.ExecEnv{}))
	body, err := json.Marshal(api.SurfaceRequest{
		RequestID: "req-mismatch",
		Context:   map[string]any{"user_id": "u1"},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/surface/test_suite.response_mismatch", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("unexpected status: got %d body=%s", rec.Code, rec.Body.String())
	}
	assertErrorResponse(t, rec, "PIPELINE_MISCONFIGURED", "pipeline_misconfigured", "pipeline misconfigured: upstream response contract mismatch")
}

func assertErrorResponse(t *testing.T, rec *httptest.ResponseRecorder, code, category, message string) {
	t.Helper()

	var resp api.ErrorResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got, want := resp.Code, code; got != want {
		t.Fatalf("unexpected code: got %q want %q", got, want)
	}
	if got, want := resp.Category, category; got != want {
		t.Fatalf("unexpected category: got %q want %q", got, want)
	}
	if got, want := resp.Error, message; got != want {
		t.Fatalf("unexpected error message: got %q want %q", got, want)
	}
}

func newTestRunner(registry *pipeline.Registry, env *runtime.ExecEnv) *runner.Runner {
	nodeRegistry := runtime.NewRegistry()
	nodes.RegisterDefaults(nodeRegistry)
	return &runner.Runner{
		Registry: registry,
		Engine:   runtime.NewEngine(nodeRegistry),
		Env:      env,
	}
}

func TestDefaultRegistryIncludesSimpleE2EPipeline(t *testing.T) {
	if err := pipeline.DefaultRegistry.BuildAll(); err != nil {
		t.Fatalf("BuildAll failed: %v", err)
	}

	registered, ok := pipeline.DefaultRegistry.Get("sandbox.simple_e2e")
	if !ok {
		t.Fatal("expected sandbox.simple_e2e in default registry")
	}
	if registered.Spec == nil {
		t.Fatal("expected built spec")
	}
	if registered.Plan == nil {
		t.Fatal("expected compiled plan")
	}
}

func TestDefaultRegistryIncludesSimpleEmbedPipeline(t *testing.T) {
	if err := pipeline.DefaultRegistry.BuildAll(); err != nil {
		t.Fatalf("BuildAll failed: %v", err)
	}

	registered, ok := pipeline.DefaultRegistry.Get("sandbox.simple_embed")
	if !ok {
		t.Fatal("expected sandbox.simple_embed in default registry")
	}
	if registered.Spec == nil {
		t.Fatal("expected built spec")
	}
	if registered.Plan == nil {
		t.Fatal("expected compiled plan")
	}
}

func TestDefaultRegistryIncludesSimpleRetrievePipeline(t *testing.T) {
	if err := pipeline.DefaultRegistry.BuildAll(); err != nil {
		t.Fatalf("BuildAll failed: %v", err)
	}

	registered, ok := pipeline.DefaultRegistry.Get("sandbox.simple_retrieve")
	if !ok {
		t.Fatal("expected sandbox.simple_retrieve in default registry")
	}
	if registered.Spec == nil {
		t.Fatal("expected built spec")
	}
	if registered.Plan == nil {
		t.Fatal("expected compiled plan")
	}
}

func TestDefaultRegistryIncludesSimpleRetrieveKGPipeline(t *testing.T) {
	if err := pipeline.DefaultRegistry.BuildAll(); err != nil {
		t.Fatalf("BuildAll failed: %v", err)
	}

	registered, ok := pipeline.DefaultRegistry.Get("sandbox.simple_retrieve_kg")
	if !ok {
		t.Fatal("expected sandbox.simple_retrieve_kg in default registry")
	}
	if registered.Spec == nil {
		t.Fatal("expected built spec")
	}
	if registered.Plan == nil {
		t.Fatal("expected compiled plan")
	}
}

func TestDefaultRegistryIncludesSimpleScorePipeline(t *testing.T) {
	if err := pipeline.DefaultRegistry.BuildAll(); err != nil {
		t.Fatalf("BuildAll failed: %v", err)
	}

	registered, ok := pipeline.DefaultRegistry.Get("sandbox.simple_score")
	if !ok {
		t.Fatal("expected sandbox.simple_score in default registry")
	}
	if registered.Spec == nil {
		t.Fatal("expected built spec")
	}
	if registered.Plan == nil {
		t.Fatal("expected compiled plan")
	}
}

func TestDefaultRegistryIncludesSimpleScoreCandidatesPipeline(t *testing.T) {
	if err := pipeline.DefaultRegistry.BuildAll(); err != nil {
		t.Fatalf("BuildAll failed: %v", err)
	}

	registered, ok := pipeline.DefaultRegistry.Get("sandbox.simple_score_candidates")
	if !ok {
		t.Fatal("expected sandbox.simple_score_candidates in default registry")
	}
	if registered.Spec == nil {
		t.Fatal("expected built spec")
	}
	if registered.Plan == nil {
		t.Fatal("expected compiled plan")
	}
}
