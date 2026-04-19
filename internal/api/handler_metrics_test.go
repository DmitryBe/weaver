package api_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dmitryBe/weaver/internal/api"
	"github.com/dmitryBe/weaver/internal/dsl/expr/request"
	"github.com/dmitryBe/weaver/internal/dsl/expr/response"
	"github.com/dmitryBe/weaver/internal/dsl/input"
	"github.com/dmitryBe/weaver/internal/dsl/op"
	"github.com/dmitryBe/weaver/internal/dsl/pipeline"
	"github.com/dmitryBe/weaver/internal/metrics"
	"github.com/dmitryBe/weaver/internal/nodes"
	"github.com/dmitryBe/weaver/internal/runner"
	"github.com/dmitryBe/weaver/internal/runtime"
)

func TestServeSurfaceRecordsRequestMetrics(t *testing.T) {
	registry := pipeline.NewRegistry()
	registry.Register("test_suite", "request_metrics", func() *pipeline.Spec {
		return pipeline.New().
			Input(input.String("city")).
			Candidates("post_rank", op.Take(1))
	})
	if err := registry.BuildAll(); err != nil {
		t.Fatalf("BuildAll failed: %v", err)
	}

	recorder := metrics.NewTestRecorder(nil)
	server := api.NewServer(newMetricsRunner(registry, &runtime.ExecEnv{Metrics: recorder}))

	body, err := json.Marshal(api.SurfaceRequest{
		Context: map[string]any{"city": "almaty"},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/surface/test_suite.request_metrics", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d body=%s", rec.Code, rec.Body.String())
	}
	if got, want := len(recorder.Requests), 1; got != want {
		t.Fatalf("unexpected request metric count: got %d want %d", got, want)
	}
	if got, want := recorder.Requests[0].Status, metrics.StatusOK; got != want {
		t.Fatalf("unexpected request metric status: got %q want %q", got, want)
	}
}

func TestServeSurfaceRecordsRequestMetricOnPipelineError(t *testing.T) {
	registry := pipeline.NewRegistry()
	registry.Register("test_suite", "request_metrics_error", func() *pipeline.Spec {
		return pipeline.New().
			Input(input.String("user_id")).
			Candidates(
				"score_candidates",
				op.Model("rank_brands").
					Request(request.Object()).
					Response(response.Scores()).
					Into("score"),
			)
	})
	if err := registry.BuildAll(); err != nil {
		t.Fatalf("BuildAll failed: %v", err)
	}

	recorder := metrics.NewTestRecorder(nil)
	server := api.NewServer(newMetricsRunner(registry, &runtime.ExecEnv{Metrics: recorder}))

	body, err := json.Marshal(api.SurfaceRequest{
		Context: map[string]any{"user_id": "u1"},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/surface/test_suite.request_metrics_error", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("unexpected status: got %d body=%s", rec.Code, rec.Body.String())
	}
	if got, want := len(recorder.Requests), 1; got != want {
		t.Fatalf("unexpected request metric count: got %d want %d", got, want)
	}
	if got, want := recorder.Requests[0].Status, metrics.StatusError; got != want {
		t.Fatalf("unexpected request metric status: got %q want %q", got, want)
	}
}

func newMetricsRunner(registry *pipeline.Registry, env *runtime.ExecEnv) *runner.Runner {
	nodeRegistry := runtime.NewRegistry()
	nodes.RegisterDefaults(nodeRegistry)
	return &runner.Runner{
		Registry: registry,
		Engine:   runtime.NewEngine(nodeRegistry),
		Env:      env,
	}
}
