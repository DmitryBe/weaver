package nodes

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/dmitryBe/weaver/internal/dsl/expr"
	"github.com/dmitryBe/weaver/internal/dsl/param"
	"github.com/dmitryBe/weaver/internal/dsl/pipeline"
	"github.com/dmitryBe/weaver/internal/dsl/retrieve"
	"github.com/dmitryBe/weaver/internal/metrics"
	"github.com/dmitryBe/weaver/internal/nodeutil"
	"github.com/dmitryBe/weaver/internal/retriever"
	"github.com/dmitryBe/weaver/internal/runtime"
)

type RetrieveSourceExecutor struct{}

func (RetrieveSourceExecutor) Execute(ctx context.Context, node pipeline.CompiledNode, in runtime.NodeInput, env *runtime.ExecEnv) (runtime.NodeOutput, error) {
	startedAt := time.Now()
	sourceType := retrieveSourceType(node.RetrieveSource)

	var out runtime.NodeOutput
	var err error
	switch current := node.RetrieveSource.(type) {
	case *retrieve.KVSpec:
		out, err = executeKV(ctx, current, in, env)
	case *retrieve.KGSpec:
		out, err = executeKG(ctx, current, in, env)
	case *retrieve.QdrantSpec:
		out, err = executeQdrant(ctx, current, in, env)
	default:
		err = fmt.Errorf("unsupported retrieve source %T", node.RetrieveSource)
	}

	env.MetricRecorder().RecordRetriever(ctx, metrics.RetrieverMetric{
		Pipeline:   in.State.Meta.Pipeline,
		StageName:  node.StageName,
		SourceType: sourceType,
		Status:     retrieverStatus(err),
		Duration:   time.Since(startedAt),
		Candidates: len(out.Candidates),
	})
	return out, err
}

func executeKV(ctx context.Context, spec *retrieve.KVSpec, in runtime.NodeInput, env *runtime.ExecEnv) (runtime.NodeOutput, error) {
	if err := nodeutil.EnsureFeatureStore(ctx, env); err != nil {
		return runtime.NodeOutput{}, err
	}
	key, err := nodeutil.EvalExpr(spec.KeyExpr, in.State, nil)
	if err != nil {
		return runtime.NodeOutput{}, err
	}
	req := retriever.KVRequest{
		Feature: spec.Feature,
		Key:     key,
		Limit:   spec.Limit,
	}
	candidates, err := retriever.RetrieveKV(ctx, req, env.FeatureStore)
	if err != nil {
		return runtime.NodeOutput{}, err
	}
	return runtime.NodeOutput{Candidates: candidates}, nil
}

func executeKG(ctx context.Context, spec *retrieve.KGSpec, in runtime.NodeInput, env *runtime.ExecEnv) (runtime.NodeOutput, error) {
	if err := nodeutil.EnsureKnowledgeGraphs(ctx, env); err != nil {
		return runtime.NodeOutput{}, err
	}

	_, graph, err := resolveKnowledgeGraph(spec.ClusterName, env.KnowledgeGraphs)
	if err != nil {
		return runtime.NodeOutput{}, err
	}

	cypher, err := loadCypher(spec.QueryPath)
	if err != nil {
		return runtime.NodeOutput{}, fmt.Errorf("kg retriever: %w", err)
	}

	params := make(map[string]any, len(spec.Bindings))
	for _, binding := range spec.Bindings {
		value, err := resolveParam(binding, in.State)
		if err != nil {
			return runtime.NodeOutput{}, fmt.Errorf("resolve kg param %q: %w", binding.Name, err)
		}
		params[binding.Name] = value
	}

	candidates, err := retriever.RetrieveKG(ctx, retriever.KGRequest{
		Query:  cypher,
		Params: params,
		Limit:  spec.Limit,
	}, graph)
	if err != nil {
		return runtime.NodeOutput{}, err
	}
	return runtime.NodeOutput{Candidates: candidates}, nil
}

func resolveKnowledgeGraph(requested string, graphs map[string]runtime.KnowledgeGraph) (string, runtime.KnowledgeGraph, error) {
	if requested != "" {
		graph, ok := graphs[requested]
		if !ok || graph == nil {
			return "", nil, fmt.Errorf("knowledge graph cluster %q is not configured", requested)
		}
		return requested, graph, nil
	}

	if graph, ok := graphs["default"]; ok && graph != nil {
		return "default", graph, nil
	}

	if len(graphs) == 1 {
		for name, graph := range graphs {
			if graph != nil {
				return name, graph, nil
			}
		}
	}

	return "", nil, fmt.Errorf("knowledge graph cluster %q is not configured", "default")
}

func executeQdrant(ctx context.Context, spec *retrieve.QdrantSpec, in runtime.NodeInput, env *runtime.ExecEnv) (runtime.NodeOutput, error) {
	_ = ctx
	_ = spec
	_ = in
	_ = env
	return runtime.NodeOutput{}, fmt.Errorf("not implemented: qdrant retriever")
}

func loadCypher(query string) (string, error) {
	if strings.HasSuffix(query, ".cypher") || strings.HasSuffix(query, ".cql") {
		b, err := os.ReadFile(query)
		if err != nil {
			return "", fmt.Errorf("load cypher file %q: %w", query, err)
		}
		return strings.TrimSpace(string(b)), nil
	}
	return query, nil
}

func resolveParam(binding param.Binding, state runtime.State) (any, error) {
	if current, ok := binding.Value.(expr.Expr); ok {
		return nodeutil.EvalExpr(current, state, nil)
	}
	return binding.Value, nil
}

func retrieveSourceType(source retrieve.Source) string {
	switch source.(type) {
	case *retrieve.KVSpec:
		return "kv"
	case *retrieve.KGSpec:
		return "kg"
	case *retrieve.QdrantSpec:
		return "qdrant"
	default:
		return "unknown"
	}
}

func retrieverStatus(err error) string {
	if err != nil {
		return metrics.StatusError
	}
	return metrics.StatusOK
}
