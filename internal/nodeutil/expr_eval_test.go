package nodeutil

import (
	"testing"

	"github.com/dmitryBe/weaver/internal/dsl/expr"
	"github.com/dmitryBe/weaver/internal/runtime"
)

func TestEvalExprContextAndCandidateRefs(t *testing.T) {
	state := runtime.State{
		Context: runtime.Context{"user_id": "u1"},
	}
	candidate := runtime.Candidate{"brand_id": 101}

	got, err := EvalExpr(expr.Context("user_id"), state, nil)
	if err != nil || got != "u1" {
		t.Fatalf("unexpected context ref result: got %v err %v", got, err)
	}

	got, err = EvalExpr(expr.Candidate("brand_id"), state, candidate)
	if err != nil || got != 101 {
		t.Fatalf("unexpected candidate ref result: got %v err %v", got, err)
	}
}

func TestEvalExprTupleAndPadLeft(t *testing.T) {
	state := runtime.State{
		Context: runtime.Context{
			"user_id": "u1",
			"items":   []int{1, 2},
		},
	}

	got, err := EvalExpr(expr.Tuple(expr.Const("brand"), expr.Context("user_id")), state, nil)
	if err != nil || got != "brand|u1" {
		t.Fatalf("unexpected tuple result: got %v err %v", got, err)
	}

	got, err = EvalExpr(expr.PadLeft(expr.Context("items"), 4, 0), state, nil)
	if err != nil {
		t.Fatalf("pad left failed: %v", err)
	}
	items, ok := got.([]any)
	if !ok || len(items) != 4 || items[0] != 0 || items[1] != 0 {
		t.Fatalf("unexpected padded result: %#v", got)
	}
}

func TestGetPathMissingPath(t *testing.T) {
	_, err := GetPath(runtime.Context{"user_id": "u1"}, []string{"missing"})
	if err == nil {
		t.Fatal("expected missing path error")
	}
}
