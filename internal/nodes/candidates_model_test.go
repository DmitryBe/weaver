package nodes

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dmitryBe/weaver/internal/dsl/expr"
	"github.com/dmitryBe/weaver/internal/dsl/expr/request"
	"github.com/dmitryBe/weaver/internal/dsl/expr/response"
	"github.com/dmitryBe/weaver/internal/dsl/op"
	"github.com/dmitryBe/weaver/internal/dsl/pipeline"
	"github.com/dmitryBe/weaver/internal/runtime"
)

func TestCandidatesModelExecutorCallsRESTEndpointAndAssignsScores(t *testing.T) {
	executor := CandidatesModelExecutor{}

	var gotPayload map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotPayload); err != nil {
			t.Fatalf("decode request: %v", err)
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{
			"scores": []float64{0.9, 0.4},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	node := pipeline.CompiledNode{
		ID:   "cand_score",
		Kind: pipeline.NodeKindCandidatesModel,
		Ops: []op.Op{
			op.Model("rank_brands").
				Endpoint(server.URL).
				Request(
					request.Object(
						request.Field("context.user_id", expr.Context("user_id")),
						request.Field(
							"request",
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
		},
	}

	out, err := executor.Execute(
		context.Background(),
		node,
		runtime.NodeInput{
			State: runtime.State{
				Context: runtime.Context{"user_id": "u1"},
				Candidates: []runtime.Candidate{
					{"brand_id": 101, "rating": 4.8},
					{"brand_id": 102, "rating": 4.5},
				},
			},
		},
		&runtime.ExecEnv{},
	)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	contextPayload, ok := gotPayload["context"].(map[string]any)
	if !ok {
		t.Fatalf("expected context payload, got %T", gotPayload["context"])
	}
	if got, want := contextPayload["user_id"], "u1"; got != want {
		t.Fatalf("unexpected context.user_id: got %#v want %#v", got, want)
	}

	items, ok := gotPayload["request"].([]any)
	if !ok {
		t.Fatalf("expected request items, got %T", gotPayload["request"])
	}
	if got, want := len(items), 2; got != want {
		t.Fatalf("unexpected request item count: got %d want %d", got, want)
	}

	firstItem, ok := items[0].(map[string]any)
	if !ok {
		t.Fatalf("expected request item object, got %T", items[0])
	}
	if got, want := firstItem["brand_id"], float64(101); got != want {
		t.Fatalf("unexpected first request brand_id: got %#v want %#v", got, want)
	}

	if got, want := out.Candidates[0]["score"], 0.9; got != want {
		t.Fatalf("unexpected first score: got %#v want %#v", got, want)
	}
	if got, want := out.Candidates[1]["score"], 0.4; got != want {
		t.Fatalf("unexpected second score: got %#v want %#v", got, want)
	}
}

func TestCandidatesModelExecutorErrorsOnMismatchedScoreCount(t *testing.T) {
	executor := CandidatesModelExecutor{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{
			"scores": []float64{0.9},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	node := pipeline.CompiledNode{
		ID:   "cand_score",
		Kind: pipeline.NodeKindCandidatesModel,
		Ops: []op.Op{
			op.Model("rank_brands").
				Endpoint(server.URL).
				Request(
					request.Object(
						request.Field(
							"request",
							request.ForEachCandidate(
								request.Object(request.Field("brand_id", expr.Candidate("brand_id"))),
							),
						),
					),
				).
				Response(response.Scores()).
				Into("score"),
		},
	}

	_, err := executor.Execute(
		context.Background(),
		node,
		runtime.NodeInput{
			State: runtime.State{
				Candidates: []runtime.Candidate{
					{"brand_id": 101},
					{"brand_id": 102},
				},
			},
		},
		&runtime.ExecEnv{},
	)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "expected 2 scores, got 1") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func equalSlices(got any, want []any) bool {
	gotSlice, ok := got.([]any)
	if !ok {
		return false
	}
	if len(gotSlice) != len(want) {
		return false
	}
	for i := range gotSlice {
		if gotSlice[i] != want[i] {
			return false
		}
	}
	return true
}

func contains(s string, part string) bool {
	return strings.Contains(s, part)
}
