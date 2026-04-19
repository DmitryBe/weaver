package nodeutil

import (
	"reflect"
	"testing"

	"github.com/dmitryBe/weaver/internal/dsl/types"
)

func TestCastValueScalars(t *testing.T) {
	got, err := CastValue("12", &types.Int)
	if err != nil || got != 12 {
		t.Fatalf("unexpected int coercion: got %v err %v", got, err)
	}

	got, err = CastValue(12, &types.Float)
	if err != nil || got != float64(12) {
		t.Fatalf("unexpected float coercion: got %v err %v", got, err)
	}
}

func TestCastValueArrayAndScores(t *testing.T) {
	got, err := CastValue([]any{float64(1), float64(2)}, &types.Type{Kind: types.KindArray, Elem: &types.Int})
	if err != nil {
		t.Fatalf("array coercion failed: %v", err)
	}
	if !reflect.DeepEqual(got, []int{1, 2}) {
		t.Fatalf("unexpected coerced array: %#v", got)
	}

	scores, err := ToScoreSlice([]float64{1, 2})
	if err != nil || len(scores) != 2 {
		t.Fatalf("unexpected score slice: %#v err %v", scores, err)
	}
}
