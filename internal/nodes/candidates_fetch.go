package nodes

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/dmitryBe/weaver/internal/dsl/op"
	"github.com/dmitryBe/weaver/internal/dsl/pipeline"
	"github.com/dmitryBe/weaver/internal/metrics"
	"github.com/dmitryBe/weaver/internal/nodeutil"
	"github.com/dmitryBe/weaver/internal/runtime"
)

type CandidatesFetchExecutor struct{}

type candidateEntityFetchPlan struct {
	// targetsByKey fans one fetched row back out to every candidate sharing
	// the same normalized key.
	features     []string
	uniqueKeys   []runtime.Key
	targetsByKey map[string][]int
}

type candidateEntityFetchRequest struct {
	entity string
	ops    []*op.FetchOp
	plan   candidateEntityFetchPlan
}

type candidateEntityFetchResult struct {
	request candidateEntityFetchRequest
	rows    []map[string]any
}

func (CandidatesFetchExecutor) Execute(ctx context.Context, node pipeline.CompiledNode, in runtime.NodeInput, env *runtime.ExecEnv) (runtime.NodeOutput, error) {
	if err := nodeutil.EnsureFeatureStore(ctx, env); err != nil {
		return runtime.NodeOutput{}, err
	}

	out := filterNilCandidates(runtime.CloneCandidates(in.State.Candidates))
	if len(out) == 0 {
		return runtime.NodeOutput{Candidates: out}, nil
	}
	entityOps, err := groupCandidateFetchOpsByEntity(node)
	if err != nil {
		return runtime.NodeOutput{}, err
	}

	requests, err := buildCandidateEntityFetchRequests(entityOps, in.State, out)
	if err != nil {
		return runtime.NodeOutput{}, err
	}

	results, err := fetchCandidateEntityResults(ctx, env, in.State.Meta.Pipeline, node.StageName, requests)
	if err != nil {
		return runtime.NodeOutput{}, err
	}

	for _, result := range results {
		if err := applyCandidateEntityFetchRows(ctx, env, in.State.Meta.Pipeline, node.StageName, node.ID, result.request.entity, result.request.ops, result.request.plan, result.rows, out); err != nil {
			return runtime.NodeOutput{}, err
		}
	}

	return runtime.NodeOutput{Candidates: out}, nil
}

func filterNilCandidates(candidates []runtime.Candidate) []runtime.Candidate {
	if len(candidates) == 0 {
		return candidates
	}
	out := make([]runtime.Candidate, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate == nil {
			continue
		}
		out = append(out, candidate)
	}
	return out
}

func groupCandidateFetchOpsByEntity(node pipeline.CompiledNode) (map[string][]*op.FetchOp, error) {
	entityOps := make(map[string][]*op.FetchOp)
	for _, raw := range node.Ops {
		current, ok := raw.(*op.FetchOp)
		if !ok {
			return nil, fmt.Errorf("node %s expected FetchOp, got %T", node.ID, raw)
		}
		entity, err := nodeutil.InferFeatureEntity(current.Source)
		if err != nil {
			return nil, err
		}
		entityOps[entity] = append(entityOps[entity], current)
	}
	return entityOps, nil
}

func buildCandidateEntityFetchRequests(entityOps map[string][]*op.FetchOp, state runtime.State, candidates []runtime.Candidate) ([]candidateEntityFetchRequest, error) {
	entities := make([]string, 0, len(entityOps))
	for entity := range entityOps {
		entities = append(entities, entity)
	}
	sort.Strings(entities)

	requests := make([]candidateEntityFetchRequest, 0, len(entities))
	for _, entity := range entities {
		ops := entityOps[entity]
		plan, err := buildCandidateEntityFetchPlan(ops, state, candidates)
		if err != nil {
			return nil, err
		}
		requests = append(requests, candidateEntityFetchRequest{
			entity: entity,
			ops:    ops,
			plan:   plan,
		})
	}
	return requests, nil
}

func buildCandidateEntityFetchPlan(ops []*op.FetchOp, state runtime.State, candidates []runtime.Candidate) (candidateEntityFetchPlan, error) {
	plan := candidateEntityFetchPlan{
		features:     make([]string, 0, len(ops)),
		uniqueKeys:   make([]runtime.Key, 0, len(candidates)),
		targetsByKey: make(map[string][]int),
	}
	seenFeatures := make(map[string]struct{}, len(ops))
	for _, current := range ops {
		if _, ok := seenFeatures[current.Source]; ok {
			continue
		}
		plan.features = append(plan.features, current.Source)
		seenFeatures[current.Source] = struct{}{}
	}

	for i := range candidates {
		candidateKeys := make([]runtime.Key, 0, len(ops))
		for _, current := range ops {
			key, err := nodeutil.BuildFeatureKey(current.Source, current.KeyExpr, state, candidates[i])
			if err != nil {
				return candidateEntityFetchPlan{}, err
			}
			candidateKeys = append(candidateKeys, key)
		}
		mergedKey, err := nodeutil.MergeFeatureKeys(candidateKeys...)
		if err != nil {
			return candidateEntityFetchPlan{}, err
		}
		normalized := nodeutil.NormalizeKey(mergedKey)
		if _, ok := plan.targetsByKey[normalized]; !ok {
			plan.targetsByKey[normalized] = nil
			plan.uniqueKeys = append(plan.uniqueKeys, mergedKey)
		}
		plan.targetsByKey[normalized] = append(plan.targetsByKey[normalized], i)
	}

	return plan, nil
}

func fetchCandidateEntityRows(ctx context.Context, env *runtime.ExecEnv, request candidateEntityFetchRequest) ([]map[string]any, error) {
	rows, err := env.FeatureStore.Fetch(ctx, request.plan.features, request.plan.uniqueKeys...)
	if err != nil {
		return nil, runtime.NewExecutionError(
			runtime.CategoryPipelineMisconfigured,
			"pipeline misconfigured: required feature definition missing",
			err,
		)
	}
	if len(rows) != len(request.plan.uniqueKeys) {
		return nil, runtime.NewExecutionError(
			runtime.CategoryPipelineMisconfigured,
			"pipeline misconfigured: feature store contract mismatch",
			fmt.Errorf("expected %d feature rows for entity %q, got %d", len(request.plan.uniqueKeys), request.entity, len(rows)),
		)
	}
	return rows, nil
}

func fetchCandidateEntityResults(ctx context.Context, env *runtime.ExecEnv, pipelineName, stageName string, requests []candidateEntityFetchRequest) ([]candidateEntityFetchResult, error) {
	return nodeutil.RunParallel(env.FeatureStoreFetchMaxParallelism, requests, func(_ int, request candidateEntityFetchRequest) (candidateEntityFetchResult, error) {
		startedAt := time.Now()
		rows, err := fetchCandidateEntityRows(ctx, env, request)
		env.MetricRecorder().RecordFeatureFetch(ctx, metrics.FeatureFetchMetric{
			Pipeline:  pipelineName,
			StageName: stageName,
			Scope:     metrics.ScopeCandidates,
			Entity:    request.entity,
			Status:    featureFetchStatus(err),
			Duration:  time.Since(startedAt),
		})
		if err != nil {
			return candidateEntityFetchResult{}, err
		}
		return candidateEntityFetchResult{
			request: request,
			rows:    rows,
		}, nil
	})
}

func applyCandidateEntityFetchRows(ctx context.Context, env *runtime.ExecEnv, pipelineName, stageName, nodeID, entity string, ops []*op.FetchOp, plan candidateEntityFetchPlan, rows []map[string]any, candidates []runtime.Candidate) error {
	for rowIndex, row := range rows {
		normalized := nodeutil.NormalizeKey(plan.uniqueKeys[rowIndex])
		for _, candidateIndex := range plan.targetsByKey[normalized] {
			for _, current := range ops {
				value, err := resolveFetchValue(ctx, env, pipelineName, stageName, metrics.ScopeCandidates, entity, current, row)
				if err != nil {
					return fmt.Errorf("%w for candidate %d in node %s", err, candidateIndex, nodeID)
				}
				castValue, err := nodeutil.CastValue(value, current.FieldType)
				if err != nil {
					return runtime.NewExecutionError(
						runtime.CategoryPipelineMisconfigured,
						"pipeline misconfigured: feature value contract mismatch",
						err,
					)
				}
				candidates[candidateIndex][current.IntoField] = castValue
			}
		}
	}
	return nil
}
