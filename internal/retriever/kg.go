package retriever

import (
	"context"
	"fmt"

	"github.com/dmitryBe/weaver/internal/runtime"
)

type KGRequest struct {
	Query  string
	Params map[string]any
	Limit  int
}

func RetrieveKG(ctx context.Context, req KGRequest, graph runtime.KnowledgeGraph) ([]runtime.Candidate, error) {
	if graph == nil {
		return nil, fmt.Errorf("knowledge graph is required")
	}
	if req.Query == "" {
		return nil, fmt.Errorf("kg query is required")
	}

	rows, err := graph.Query(ctx, req.Query, req.Params)
	if err != nil {
		return nil, err
	}

	candidates, err := decodeCandidatesFromRows(rows)
	if err != nil {
		return nil, err
	}
	if req.Limit > 0 && len(candidates) > req.Limit {
		candidates = candidates[:req.Limit]
	}
	return candidates, nil
}
