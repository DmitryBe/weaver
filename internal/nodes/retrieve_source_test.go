package nodes

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dmitryBe/weaver/internal/adapters/featurestore"
	"github.com/dmitryBe/weaver/internal/dsl/expr"
	"github.com/dmitryBe/weaver/internal/dsl/param"
	"github.com/dmitryBe/weaver/internal/dsl/pipeline"
	"github.com/dmitryBe/weaver/internal/dsl/retrieve"
	"github.com/dmitryBe/weaver/internal/runtime"
)

func TestRetrieveSourceExecutor_KV_EndToEndWithFileStore(t *testing.T) {
	executor := RetrieveSourceExecutor{}
	store := featurestore.NewFileStore(filepath.Join("..", "..", "testdata", "featurestore", "unit"))

	node := pipeline.CompiledNode{
		ID:             "retrieve_candidates:source:0",
		Kind:           pipeline.NodeKindRetrieveSource,
		RetrieveSource: retrieve.KV("trending/brands").Key(expr.Tuple(expr.Context("city"), expr.Context("vertical"))).TopK(2),
	}

	out, err := executor.Execute(
		context.Background(),
		node,
		runtime.NodeInput{
			State: runtime.State{
				Context: runtime.Context{
					"city":     "almaty",
					"vertical": "food",
				},
			},
		},
		&runtime.ExecEnv{
			FeatureStore: store,
		},
	)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if got, want := len(out.Candidates), 2; got != want {
		t.Fatalf("unexpected candidate count: got %d want %d", got, want)
	}
	if out.Candidates[0]["brand_id"] != float64(101) && out.Candidates[0]["brand_id"] != 101 {
		t.Fatalf("unexpected first brand_id: %#v", out.Candidates[0]["brand_id"])
	}
}

func TestRetrieveSourceExecutor_KV_ReturnsEmptyWhenRowMissing(t *testing.T) {
	executor := RetrieveSourceExecutor{}
	store := featurestore.NewFileStore(filepath.Join("..", "..", "testdata", "featurestore", "unit"))

	node := pipeline.CompiledNode{
		ID:             "retrieve_candidates:source:0",
		Kind:           pipeline.NodeKindRetrieveSource,
		RetrieveSource: retrieve.KV("trending/brands").Key(expr.Tuple(expr.Context("city"), expr.Context("vertical"))).TopK(2),
	}

	out, err := executor.Execute(
		context.Background(),
		node,
		runtime.NodeInput{
			State: runtime.State{
				Context: runtime.Context{
					"city":     "dubai",
					"vertical": "food",
				},
			},
		},
		&runtime.ExecEnv{
			FeatureStore: store,
		},
	)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if got, want := len(out.Candidates), 0; got != want {
		t.Fatalf("unexpected candidate count: got %d want %d", got, want)
	}
}

type recordingKnowledgeGraph struct {
	query  string
	params map[string]any
	rows   []map[string]any
}

func (g *recordingKnowledgeGraph) Query(ctx context.Context, query string, params map[string]any) ([]map[string]any, error) {
	_ = ctx
	g.query = query
	g.params = params
	return g.rows, nil
}

func (*recordingKnowledgeGraph) Close(ctx context.Context) error {
	_ = ctx
	return nil
}

func TestRetrieveSourceExecutor_KG_EndToEnd(t *testing.T) {
	executor := RetrieveSourceExecutor{}
	queryDir := t.TempDir()
	queryPath := filepath.Join(queryDir, "popular_brands.cypher")
	if err := os.WriteFile(queryPath, []byte("RETURN $city AS city"), 0o600); err != nil {
		t.Fatalf("write query file: %v", err)
	}
	graph := &recordingKnowledgeGraph{
		rows: []map[string]any{
			{"brand_id": 201, "score": 0.95, "brand_name": "Graph Brand"},
			{"brand_id": 202, "score": 0.85, "brand_name": "Backup Brand"},
		},
	}
	node := pipeline.CompiledNode{
		ID:   "retrieve_candidates:source:1",
		Kind: pipeline.NodeKindRetrieveSource,
		RetrieveSource: retrieve.KG(queryPath).
			Params(
				param.String("city", expr.Context("city")),
				param.Int("limit", 2),
			).
			Cluster("cluster1").
			TopK(1),
	}

	out, err := executor.Execute(
		context.Background(),
		node,
		runtime.NodeInput{
			State: runtime.State{
				Context: runtime.Context{
					"city": "almaty",
				},
			},
		},
		&runtime.ExecEnv{
			KnowledgeGraphs: map[string]runtime.KnowledgeGraph{
				"cluster1": graph,
			},
		},
	)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if got, want := len(out.Candidates), 1; got != want {
		t.Fatalf("unexpected candidate count: got %d want %d", got, want)
	}
	if out.Candidates[0]["brand_name"] != "Graph Brand" {
		t.Fatalf("unexpected candidate payload: %#v", out.Candidates[0])
	}
	if graph.query != "RETURN $city AS city" {
		t.Fatalf("unexpected query text: %q", graph.query)
	}
	if graph.params["city"] != "almaty" || graph.params["limit"] != 2 {
		t.Fatalf("unexpected params: %#v", graph.params)
	}
}

func TestRetrieveSourceExecutor_KG_ErrorsWithoutKnowledgeGraphs(t *testing.T) {
	executor := RetrieveSourceExecutor{}
	node := pipeline.CompiledNode{
		ID:             "retrieve_candidates:source:1",
		Kind:           pipeline.NodeKindRetrieveSource,
		RetrieveSource: retrieve.KG("queries/kg/popular_brands.cypher"),
	}

	_, err := executor.Execute(context.Background(), node, runtime.NodeInput{State: runtime.State{}}, &runtime.ExecEnv{})
	if err == nil || !strings.Contains(err.Error(), "knowledge graph clients are required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRetrieveSourceExecutor_KG_ErrorsOnUnknownCluster(t *testing.T) {
	executor := RetrieveSourceExecutor{}
	node := pipeline.CompiledNode{
		ID:             "retrieve_candidates:source:1",
		Kind:           pipeline.NodeKindRetrieveSource,
		RetrieveSource: retrieve.KG("queries/kg/popular_brands.cypher").Cluster("cluster1"),
	}

	_, err := executor.Execute(
		context.Background(),
		node,
		runtime.NodeInput{State: runtime.State{}},
		&runtime.ExecEnv{
			KnowledgeGraphs: map[string]runtime.KnowledgeGraph{
				"main": &recordingKnowledgeGraph{},
			},
		},
	)
	if err == nil || !strings.Contains(err.Error(), `knowledge graph cluster "cluster1" is not configured`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRetrieveSourceExecutor_KG_ErrorsOnUnreadableQuery(t *testing.T) {
	executor := RetrieveSourceExecutor{}
	node := pipeline.CompiledNode{
		ID:             "retrieve_candidates:source:1",
		Kind:           pipeline.NodeKindRetrieveSource,
		RetrieveSource: retrieve.KG("queries/kg/does-not-exist.cypher"),
	}

	_, err := executor.Execute(
		context.Background(),
		node,
		runtime.NodeInput{State: runtime.State{}},
		&runtime.ExecEnv{
			KnowledgeGraphs: map[string]runtime.KnowledgeGraph{
				"default": &recordingKnowledgeGraph{},
			},
		},
	)
	if err == nil || !strings.Contains(err.Error(), `queries/kg/does-not-exist.cypher`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRetrieveSourceExecutor_Qdrant_NotImplemented(t *testing.T) {
	executor := RetrieveSourceExecutor{}
	node := pipeline.CompiledNode{
		ID:             "retrieve_candidates:source:2",
		Kind:           pipeline.NodeKindRetrieveSource,
		RetrieveSource: retrieve.Qdrant("index_name").TopK(10),
	}

	_, err := executor.Execute(context.Background(), node, runtime.NodeInput{State: runtime.State{}}, &runtime.ExecEnv{})
	if err == nil {
		t.Fatal("expected error")
	}
}
