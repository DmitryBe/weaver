package nodes

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dmitryBe/weaver/internal/dsl/expr"
	"github.com/dmitryBe/weaver/internal/dsl/expr/request"
	"github.com/dmitryBe/weaver/internal/dsl/expr/response"
	"github.com/dmitryBe/weaver/internal/dsl/op"
	"github.com/dmitryBe/weaver/internal/dsl/pipeline"
	"github.com/dmitryBe/weaver/internal/runtime"
)

func TestContextModelExecutorCallsRESTEndpointAndStoresParsedValue(t *testing.T) {
	executor := ContextModelExecutor{}

	var gotMethod string
	var gotContentType string
	var gotPayload map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotContentType = r.Header.Get("Content-Type")
		if err := json.NewDecoder(r.Body).Decode(&gotPayload); err != nil {
			t.Fatalf("decode request: %v", err)
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{
			"response": [][]float64{{0.1, 0.2, 0.3}},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	node := pipeline.CompiledNode{
		ID:   "ctx_embed",
		Kind: pipeline.NodeKindContextModel,
		Ops: []op.Op{
			op.Model("embed_user").
				Endpoint(server.URL).
				Request(
					request.Object(
						request.Field("user_id", expr.Context("user_id")),
						request.Field("history.brand_ids", expr.Context("user_history_brand_ids")),
						request.Field("history.actions", expr.Context("user_history_actions")),
					),
				).
				Response(response.Path("response.0")).
				Into("user_embedding"),
		},
	}

	out, err := executor.Execute(
		context.Background(),
		node,
		runtime.NodeInput{
			State: runtime.State{
				Context: runtime.Context{
					"user_id":                "u1",
					"user_history_brand_ids": []int{101, 102},
					"user_history_actions":   []int{7, 8},
				},
			},
		},
		&runtime.ExecEnv{},
	)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if got, want := gotMethod, http.MethodPost; got != want {
		t.Fatalf("unexpected method: got %q want %q", got, want)
	}
	if got, want := gotContentType, "application/json"; got != want {
		t.Fatalf("unexpected content type: got %q want %q", got, want)
	}
	if got, want := gotPayload["user_id"], "u1"; got != want {
		t.Fatalf("unexpected user_id: got %#v want %#v", got, want)
	}

	history, ok := gotPayload["history"].(map[string]any)
	if !ok {
		t.Fatalf("expected nested history object, got %T", gotPayload["history"])
	}
	if got, want := history["brand_ids"], []any{float64(101), float64(102)}; !equalSlices(got, want) {
		t.Fatalf("unexpected history.brand_ids: got %#v want %#v", got, want)
	}
	if got, want := history["actions"], []any{float64(7), float64(8)}; !equalSlices(got, want) {
		t.Fatalf("unexpected history.actions: got %#v want %#v", got, want)
	}

	embedding, ok := out.Context["user_embedding"].([]any)
	if !ok {
		t.Fatalf("expected embedding to be []any, got %T", out.Context["user_embedding"])
	}
	if got, want := embedding, []any{0.1, 0.2, 0.3}; !equalSlices(got, want) {
		t.Fatalf("unexpected embedding: got %#v want %#v", got, want)
	}
}

func TestContextModelExecutorReturnsHTTPErrors(t *testing.T) {
	executor := ContextModelExecutor{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "embed unavailable", http.StatusBadGateway)
	}))
	defer server.Close()

	node := pipeline.CompiledNode{
		ID:   "ctx_embed",
		Kind: pipeline.NodeKindContextModel,
		Ops: []op.Op{
			op.Model("embed_user").
				Endpoint(server.URL).
				Request(request.Object(request.Field("user_id", expr.Context("user_id")))).
				Response(response.Path("response.0")).
				Into("user_embedding"),
		},
	}

	_, err := executor.Execute(
		context.Background(),
		node,
		runtime.NodeInput{
			State: runtime.State{
				Context: runtime.Context{"user_id": "u1"},
			},
		},
		&runtime.ExecEnv{},
	)
	if err == nil {
		t.Fatal("expected error")
	}
	if got, want := err.Error(), `returned status 502`; !contains(got, want) {
		t.Fatalf("unexpected error: %v", err)
	}
}
