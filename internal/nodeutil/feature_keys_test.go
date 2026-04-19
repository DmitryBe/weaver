package nodeutil

import (
	"strings"
	"testing"

	"github.com/dmitryBe/weaver/internal/dsl/expr"
	"github.com/dmitryBe/weaver/internal/runtime"
)

func TestInferFeatureEntity(t *testing.T) {
	got, err := InferFeatureEntity("brand_user/score")
	if err != nil || got != "brand_user" {
		t.Fatalf("unexpected entity: got %q err %v", got, err)
	}
}

func TestBuildFeatureKey(t *testing.T) {
	state := runtime.State{
		Context: runtime.Context{"user_id": "u1"},
	}
	candidate := runtime.Candidate{"brand_id": 101}

	key, err := BuildFeatureKey("brand_user/score", expr.Tuple(expr.Candidate("brand_id"), expr.Context("user_id")), state, candidate)
	if err != nil {
		t.Fatalf("build key failed: %v", err)
	}
	if key["brand_id"] != "101" || key["user_id"] != "u1" {
		t.Fatalf("unexpected key: %#v", key)
	}
}

func TestMergeFeatureKeysConflict(t *testing.T) {
	_, err := MergeFeatureKeys(
		runtime.Key{"user_id": "u1"},
		runtime.Key{"user_id": "u2"},
	)
	if err == nil {
		t.Fatal("expected merge conflict")
	}
}

func TestBuildFeatureKeyFailsForUnregisteredEntity(t *testing.T) {
	_, err := BuildFeatureKey("user1/segment", expr.Context("user_id"), runtime.State{}, nil)
	if err == nil {
		t.Fatal("expected unregistered entity error")
	}
	if !strings.Contains(err.Error(), `feature entity "user1" is not registered for "user1/segment"`) {
		t.Fatalf("unexpected error: %v", err)
	}
}
