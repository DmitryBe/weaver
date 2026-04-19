package featurestore

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/dmitryBe/weaver/internal/runtime"
)

func TestFileStoreFetchSimpleKey(t *testing.T) {
	store := NewFileStore(filepath.Join("..", "..", "..", "testdata", "featurestore", "unit"))

	rows, err := store.Fetch(context.Background(), []string{"user/segment"}, runtime.Key{"user_id": "u1"})
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}

	if got, want := rows[0]["user/segment"], 0.75; got != want {
		t.Fatalf("unexpected value: got %v want %v", got, want)
	}
}

func TestFileStoreFetchCompositeKeyUsesSortedKeyParts(t *testing.T) {
	store := NewFileStore(filepath.Join("..", "..", "..", "testdata", "featurestore", "unit"))

	rows, err := store.Fetch(
		context.Background(),
		[]string{"brand_user/score"},
		runtime.Key{"user_id": "u1", "brand_id": "101"},
	)
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}

	if got, want := rows[0]["brand_user/score"], 0.9; got != want {
		t.Fatalf("unexpected value: got %v want %v", got, want)
	}
}

func TestFileStoreFetchRejectsMixedEntities(t *testing.T) {
	store := NewFileStore(filepath.Join("..", "..", "..", "testdata", "featurestore", "unit"))

	_, err := store.Fetch(
		context.Background(),
		[]string{"user/segment", "brand/rating"},
		runtime.Key{"user_id": "u1"},
	)
	if err == nil {
		t.Fatal("expected mixed entity fetch to fail")
	}
}

func TestFileStoreFetchOmitsMissingFeaturesAndPreservesOrder(t *testing.T) {
	store := NewFileStore(filepath.Join("..", "..", "..", "testdata", "featurestore", "unit"))

	rows, err := store.Fetch(
		context.Background(),
		[]string{"brand/rating", "brand/price_tier"},
		runtime.Key{"brand_id": "999"},
		runtime.Key{"brand_id": "101"},
	)
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}

	if got, want := len(rows), 2; got != want {
		t.Fatalf("unexpected row count: got %d want %d", got, want)
	}
	if got, want := rows[0], map[string]any{}; !reflect.DeepEqual(got, want) {
		t.Fatalf("expected empty first row, got %#v", got)
	}
	if got, want := rows[1]["brand/rating"], 4.8; got != want {
		t.Fatalf("unexpected rating: got %v want %v", got, want)
	}
}
