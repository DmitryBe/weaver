package retriever

import (
	"context"
	"errors"
	"testing"

	"github.com/dmitryBe/weaver/internal/runtime"
)

type fakeKnowledgeGraph struct {
	rows   []map[string]any
	err    error
	query  string
	params map[string]any
}

func (g *fakeKnowledgeGraph) Query(ctx context.Context, query string, params map[string]any) ([]map[string]any, error) {
	_ = ctx
	g.query = query
	g.params = params
	return g.rows, g.err
}

func (*fakeKnowledgeGraph) Close(ctx context.Context) error {
	_ = ctx
	return nil
}

func TestRetrieveKG_PreservesOrderAndTruncates(t *testing.T) {
	graph := &fakeKnowledgeGraph{
		rows: []map[string]any{
			{"brand_id": 101, "score": 0.9, "brand_name": "A"},
			{"brand_id": 102, "score": 0.8, "brand_name": "B"},
			{"brand_id": 103, "score": 0.7, "brand_name": "C"},
		},
	}

	got, err := RetrieveKG(context.Background(), KGRequest{
		Query:  "RETURN 1",
		Params: map[string]any{"city_id": "almaty"},
		Limit:  2,
	}, graph)
	if err != nil {
		t.Fatalf("RetrieveKG failed: %v", err)
	}
	if gotLen, want := len(got), 2; gotLen != want {
		t.Fatalf("unexpected candidate count: got %d want %d", gotLen, want)
	}
	if got[0]["brand_id"] != 101 || got[1]["brand_id"] != 102 {
		t.Fatalf("order not preserved: %#v", got)
	}
	if got[0]["brand_name"] != "A" {
		t.Fatalf("expected extra field passthrough, got %#v", got[0])
	}
	if graph.query != "RETURN 1" {
		t.Fatalf("unexpected query: %q", graph.query)
	}
	if graph.params["city_id"] != "almaty" {
		t.Fatalf("unexpected params: %#v", graph.params)
	}
}

func TestRetrieveKG_ReturnsAllWhenLimitNonPositive(t *testing.T) {
	graph := &fakeKnowledgeGraph{
		rows: []map[string]any{
			{"brand_id": 101, "score": 0.9},
		},
	}

	got, err := RetrieveKG(context.Background(), KGRequest{
		Query: "RETURN 1",
		Limit: 0,
	}, graph)
	if err != nil {
		t.Fatalf("RetrieveKG failed: %v", err)
	}
	if gotLen, want := len(got), 1; gotLen != want {
		t.Fatalf("unexpected candidate count: got %d want %d", gotLen, want)
	}
}

func TestRetrieveKG_ErrorsOnMissingBrandID(t *testing.T) {
	graph := &fakeKnowledgeGraph{
		rows: []map[string]any{
			{"score": 0.9},
		},
	}

	_, err := RetrieveKG(context.Background(), KGRequest{Query: "RETURN 1"}, graph)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRetrieveKG_ErrorsOnMissingScore(t *testing.T) {
	graph := &fakeKnowledgeGraph{
		rows: []map[string]any{
			{"brand_id": 101},
		},
	}

	_, err := RetrieveKG(context.Background(), KGRequest{Query: "RETURN 1"}, graph)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRetrieveKG_RequiresKnowledgeGraph(t *testing.T) {
	_, err := RetrieveKG(context.Background(), KGRequest{Query: "RETURN 1"}, nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRetrieveKG_PropagatesQueryErrors(t *testing.T) {
	graph := &fakeKnowledgeGraph{
		err: errors.New("boom"),
	}

	_, err := RetrieveKG(context.Background(), KGRequest{Query: "RETURN 1"}, graph)
	if err == nil || err.Error() != "boom" {
		t.Fatalf("unexpected error: %v", err)
	}
}

var _ runtime.KnowledgeGraph = (*fakeKnowledgeGraph)(nil)
