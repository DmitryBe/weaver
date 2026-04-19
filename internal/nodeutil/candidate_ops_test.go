package nodeutil

import (
	"reflect"
	"testing"

	"github.com/dmitryBe/weaver/internal/runtime"
)

func TestSortCandidatesByField(t *testing.T) {
	candidates := []runtime.Candidate{
		{"score": 1.0},
		{"score": 3.0},
		{"score": 2.0},
	}

	SortCandidatesByField(candidates, "score", true)
	got := []float64{
		candidates[0]["score"].(float64),
		candidates[1]["score"].(float64),
		candidates[2]["score"].(float64),
	}
	if want := []float64{3, 2, 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected sort order: got %#v want %#v", got, want)
	}
}

func TestApplyMerge(t *testing.T) {
	candidates := []runtime.Candidate{
		{"brand_id": 101},
		{"brand_id": 101},
		{"brand_id": 102},
	}
	got := DedupCandidatesByField(candidates, "brand_id")
	if len(got) != 2 {
		t.Fatalf("unexpected merged length: %d", len(got))
	}
}
