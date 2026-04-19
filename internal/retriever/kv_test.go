package retriever

import (
	"context"
	"testing"

	"github.com/dmitryBe/weaver/internal/runtime"
)

type fakeFeatureStore struct {
	rows []map[string]any
	err  error
}

func (s fakeFeatureStore) Fetch(ctx context.Context, features []string, keys ...runtime.Key) ([]map[string]any, error) {
	_ = ctx
	_ = features
	_ = keys
	return s.rows, s.err
}

func TestRetrieveKV_PreservesOrderAndTruncates(t *testing.T) {
	store := fakeFeatureStore{
		rows: []map[string]any{
			{
				"trending/brands": []any{
					map[string]any{"brand_id": 101, "score": 0.9, "extra": "a"},
					map[string]any{"brand_id": 102, "score": 0.8, "extra": "b"},
					map[string]any{"brand_id": 103, "score": 0.7, "extra": "c"},
				},
			},
		},
	}

	got, err := RetrieveKV(context.Background(), KVRequest{
		Feature: "trending/brands",
		Key:     "almaty|food",
		Limit:   2,
	}, store)
	if err != nil {
		t.Fatalf("RetrieveKV failed: %v", err)
	}
	if got, want := len(got), 2; got != want {
		t.Fatalf("unexpected candidate count: got %d want %d", got, want)
	}
	if got[0]["brand_id"] != 101 || got[1]["brand_id"] != 102 {
		t.Fatalf("order not preserved: %#v", got)
	}
	if got[0]["extra"] != "a" {
		t.Fatalf("expected extra field passthrough, got %#v", got[0])
	}
}

func TestRetrieveKV_ReturnsAllWhenLimitNonPositive(t *testing.T) {
	store := fakeFeatureStore{
		rows: []map[string]any{
			{"trending/brands": []any{map[string]any{"brand_id": 101, "score": 0.9}}},
		},
	}
	got, err := RetrieveKV(context.Background(), KVRequest{
		Feature: "trending/brands",
		Key:     "k",
		Limit:   0,
	}, store)
	if err != nil {
		t.Fatalf("RetrieveKV failed: %v", err)
	}
	if got, want := len(got), 1; got != want {
		t.Fatalf("unexpected candidate count: got %d want %d", got, want)
	}
}

func TestRetrieveKV_ReturnsEmptyOnMissingFeatureValueInRow(t *testing.T) {
	store := fakeFeatureStore{rows: []map[string]any{{}}}
	got, err := RetrieveKV(context.Background(), KVRequest{
		Feature: "trending/brands",
		Key:     "k",
	}, store)
	if err != nil {
		t.Fatalf("RetrieveKV failed: %v", err)
	}
	if got, want := len(got), 0; got != want {
		t.Fatalf("unexpected candidate count: got %d want %d", got, want)
	}
}

func TestRetrieveKV_ErrorsOnNonArrayStoredValue(t *testing.T) {
	store := fakeFeatureStore{rows: []map[string]any{{"trending/brands": map[string]any{}}}}
	_, err := RetrieveKV(context.Background(), KVRequest{
		Feature: "trending/brands",
		Key:     "k",
	}, store)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRetrieveKV_ErrorsOnCandidateMissingBrandID(t *testing.T) {
	store := fakeFeatureStore{
		rows: []map[string]any{
			{"trending/brands": []any{map[string]any{"score": 0.9}}},
		},
	}
	_, err := RetrieveKV(context.Background(), KVRequest{
		Feature: "trending/brands",
		Key:     "k",
	}, store)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRetrieveKV_ErrorsOnCandidateMissingScore(t *testing.T) {
	store := fakeFeatureStore{
		rows: []map[string]any{
			{"trending/brands": []any{map[string]any{"brand_id": 101}}},
		},
	}
	_, err := RetrieveKV(context.Background(), KVRequest{
		Feature: "trending/brands",
		Key:     "k",
	}, store)
	if err == nil {
		t.Fatal("expected error")
	}
}
