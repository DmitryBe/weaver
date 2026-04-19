package nodeutil

import (
	"reflect"
	"testing"

	"github.com/dmitryBe/weaver/internal/dsl/expr"
	"github.com/dmitryBe/weaver/internal/dsl/expr/request"
	"github.com/dmitryBe/weaver/internal/dsl/expr/response"
	"github.com/dmitryBe/weaver/internal/runtime"
)

func TestBuildRequestObjectAndForEachCandidate(t *testing.T) {
	state := runtime.State{
		Context: runtime.Context{"user_id": "u1"},
		Candidates: []runtime.Candidate{
			{"id": "a"},
			{"id": "b"},
		},
	}

	payload, err := BuildRequest(
		request.Object(
			request.Field("context.user_id", expr.Context("user_id")),
			request.Field("items", request.ForEachCandidate(
				request.Object(
					request.Field("id", expr.Candidate("id")),
				),
			)),
		),
		state,
		nil,
	)
	if err != nil {
		t.Fatalf("build request failed: %v", err)
	}

	root := payload.(map[string]any)
	contextMap := root["context"].(map[string]any)
	if contextMap["user_id"] != "u1" {
		t.Fatalf("unexpected context payload: %#v", root)
	}
	items := root["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("unexpected item count: %#v", items)
	}
}

func TestParseResponsePath(t *testing.T) {
	got, err := ParseResponse(response.Path("nested.value"), map[string]any{
		"nested": map[string]any{"value": 42},
	})
	if err != nil || got != 42 {
		t.Fatalf("unexpected parsed response: got %v err %v", got, err)
	}
}

func TestParseResponsePathWithArrayIndex(t *testing.T) {
	got, err := ParseResponse(response.Path("response.0"), map[string]any{
		"response": []any{
			[]any{0.1, 0.2, 0.3},
			[]any{0.4, 0.5, 0.6},
		},
	})
	if err != nil {
		t.Fatalf("parse response failed: %v", err)
	}
	if got, want := got, []any{0.1, 0.2, 0.3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected parsed response: got %#v want %#v", got, want)
	}
}

func TestParseResponsePathWithArrayIndexAndNestedField(t *testing.T) {
	got, err := ParseResponse(response.Path("response.0.embedding"), map[string]any{
		"response": []any{
			map[string]any{"embedding": []any{0.1, 0.2, 0.3}},
			map[string]any{"embedding": []any{0.4, 0.5, 0.6}},
		},
	})
	if err != nil {
		t.Fatalf("parse response failed: %v", err)
	}
	if got, want := got, []any{0.1, 0.2, 0.3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected parsed response: got %#v want %#v", got, want)
	}
}
