package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dmitryBe/weaver/internal/dsl/pipeline"
	"github.com/dmitryBe/weaver/internal/metrics"
)

type Engine struct {
	Registry       *Registry
	MaxParallelism int
}

func NewEngine(registry *Registry) *Engine {
	return &Engine{Registry: registry}
}

func (e *Engine) Run(ctx context.Context, plan *pipeline.CompiledPlan, initial State, env *ExecEnv) (State, error) {
	result, err := e.RunWithOptions(ctx, plan, initial, env, RunOptions{})
	if err != nil {
		return State{}, err
	}
	return result.State, nil
}

func (e *Engine) RunWithOptions(ctx context.Context, plan *pipeline.CompiledPlan, initial State, env *ExecEnv, opts RunOptions) (RunResult, error) {
	if plan == nil {
		return RunResult{}, fmt.Errorf("compiled plan must not be nil")
	}
	if e.Registry == nil {
		return RunResult{}, fmt.Errorf("runtime registry must not be nil")
	}
	if env == nil {
		return RunResult{}, fmt.Errorf("execution environment must not be nil")
	}

	state := CloneState(initial)
	if state.Meta.Pipeline == "" {
		state.Meta.Pipeline = plan.SpecName
	}

	nodesByID := make(map[string]pipeline.CompiledNode, len(plan.Nodes))
	for _, node := range plan.Nodes {
		nodesByID[node.ID] = node
	}

	outputs := make(map[string]NodeOutput, len(plan.Nodes))
	var debugInfo *DebugInfo
	if opts.Debug {
		debugInfo = &DebugInfo{Steps: make([]DebugStep, 0, len(plan.Groups))}
	}
	var activeStage *stageTimer
	for _, group := range plan.Groups {
		currentStage := stageForGroup(group, nodesByID)
		activeStage = transitionStage(ctx, env, state.Meta.Pipeline, activeStage, currentStage, false)
		groupStartedAt := time.Now()
		results, err := e.runGroup(ctx, group, nodesByID, outputs, state, env)
		if err != nil {
			activeStage = transitionStageWithDuration(ctx, env, state.Meta.Pipeline, activeStage, currentStage, metrics.StatusError, time.Since(groupStartedAt), true)
			return RunResult{}, err
		}
		for id, out := range results {
			outputs[id] = CloneNodeOutput(out)
		}
		if len(group.NodeIDs) == 1 {
			node := nodesByID[group.NodeIDs[0]]
			state = applyNodeOutput(state, node, results[group.NodeIDs[0]])
			if debugInfo != nil {
				if step, ok := snapshotStep(node, state); ok {
					debugInfo.Steps = append(debugInfo.Steps, step)
				}
			}
		}

		nextStage := nextStageForPlanGroup(plan.Groups, group.Index+1, nodesByID)
		activeStage = transitionStage(ctx, env, state.Meta.Pipeline, activeStage, nextStage, true)
	}
	transitionStage(ctx, env, state.Meta.Pipeline, activeStage, nil, true)

	return RunResult{
		State:     state,
		DebugInfo: debugInfo,
	}, nil
}

func (e *Engine) runGroup(
	ctx context.Context,
	group pipeline.ExecutionGroup,
	nodesByID map[string]pipeline.CompiledNode,
	outputs map[string]NodeOutput,
	state State,
	env *ExecEnv,
) (map[string]NodeOutput, error) {
	results := make(map[string]NodeOutput, len(group.NodeIDs))
	if len(group.NodeIDs) == 1 {
		node := nodesByID[group.NodeIDs[0]]
		out, err := e.executeNode(ctx, node, outputs, state, env)
		if err != nil {
			return nil, err
		}
		results[node.ID] = out
		return results, nil
	}

	limit := e.MaxParallelism
	if limit <= 0 || limit > len(group.NodeIDs) {
		limit = len(group.NodeIDs)
	}

	type result struct {
		id  string
		out NodeOutput
		err error
	}

	sem := make(chan struct{}, limit)
	resultCh := make(chan result, len(group.NodeIDs))
	var wg sync.WaitGroup
	for _, nodeID := range group.NodeIDs {
		node := nodesByID[nodeID]
		wg.Add(1)
		go func(node pipeline.CompiledNode) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			out, err := e.executeNode(ctx, node, outputs, state, env)
			resultCh <- result{id: node.ID, out: out, err: err}
		}(node)
	}

	wg.Wait()
	close(resultCh)

	for current := range resultCh {
		if current.err != nil {
			return nil, current.err
		}
		results[current.id] = current.out
	}

	return results, nil
}

func (e *Engine) executeNode(
	ctx context.Context,
	node pipeline.CompiledNode,
	outputs map[string]NodeOutput,
	state State,
	env *ExecEnv,
) (NodeOutput, error) {
	executor, err := e.Registry.ExecutorFor(node.Kind)
	if err != nil {
		return NodeOutput{}, err
	}

	input := NodeInput{
		State:     CloneState(state),
		Upstreams: make(map[string]NodeOutput, len(node.DependsOn)),
	}
	for _, dep := range node.DependsOn {
		out, ok := outputs[dep]
		if !ok {
			return NodeOutput{}, fmt.Errorf("missing upstream output for dependency %q", dep)
		}
		input.Upstreams[dep] = CloneNodeOutput(out)
	}

	startedAt := time.Now()
	out, err := e.executeNodeWithResilience(ctx, executor, node, input, env)
	env.MetricRecorder().RecordNode(ctx, metrics.NodeMetric{
		Pipeline:  state.Meta.Pipeline,
		StageName: node.StageName,
		NodeKind:  string(node.Kind),
		Status:    nodeMetricStatus(err),
		Duration:  time.Since(startedAt),
	})
	return out, err
}

func (e *Engine) executeNodeWithResilience(
	ctx context.Context,
	executor NodeExecutor,
	node pipeline.CompiledNode,
	input NodeInput,
	env *ExecEnv,
) (NodeOutput, error) {
	if node.Resilience == nil {
		return executor.Execute(ctx, node, input, env)
	}

	attempts := 0
	budgetCtx, cancel := context.WithTimeout(ctx, node.Resilience.MaxLatency)
	defer cancel()

	for {
		attempts++
		out, err := executor.Execute(budgetCtx, node, input, env)
		if err == nil {
			reason := metrics.ReasonFirstAttemptSuccess
			if attempts > 1 {
				reason = metrics.ReasonRetriedSuccess
			}
			recordResilienceMetric(budgetCtx, env, input.State.Meta.Pipeline, node, attempts, metrics.StatusOK, reason)
			return out, nil
		}

		switch {
		case errors.Is(err, context.DeadlineExceeded), errors.Is(budgetCtx.Err(), context.DeadlineExceeded):
			recordResilienceMetric(budgetCtx, env, input.State.Meta.Pipeline, node, attempts, metrics.StatusError, metrics.ReasonTimeout)
			return NodeOutput{}, err
		case errors.Is(err, context.Canceled), errors.Is(budgetCtx.Err(), context.Canceled):
			recordResilienceMetric(budgetCtx, env, input.State.Meta.Pipeline, node, attempts, metrics.StatusError, metrics.ReasonCanceled)
			return NodeOutput{}, err
		case attempts > node.Resilience.MaxRetries:
			recordResilienceMetric(budgetCtx, env, input.State.Meta.Pipeline, node, attempts, metrics.StatusError, metrics.ReasonRetriesExhausted)
			return NodeOutput{}, err
		}

		if err := sleepBackoff(budgetCtx, attempts); err != nil {
			reason := metrics.ReasonCanceled
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(budgetCtx.Err(), context.DeadlineExceeded) {
				reason = metrics.ReasonTimeout
			}
			recordResilienceMetric(budgetCtx, env, input.State.Meta.Pipeline, node, attempts, metrics.StatusError, reason)
			return NodeOutput{}, err
		}
	}
}

func sleepBackoff(ctx context.Context, attempt int) error {
	timer := time.NewTimer(time.Duration(attempt) * 10 * time.Millisecond)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func recordResilienceMetric(ctx context.Context, env *ExecEnv, pipelineName string, node pipeline.CompiledNode, attempts int, status, reason string) {
	env.MetricRecorder().RecordResilience(ctx, metrics.ResilienceMetric{
		Pipeline:  pipelineName,
		StageName: node.StageName,
		NodeKind:  string(node.Kind),
		Status:    status,
		Reason:    reason,
		Attempts:  attempts,
	})
}

func applyNodeOutput(state State, node pipeline.CompiledNode, out NodeOutput) State {
	next := state
	switch node.Kind {
	case pipeline.NodeKindContextFetch, pipeline.NodeKindContextModel, pipeline.NodeKindContextMutate:
		next.Context = CloneContext(out.Context)
	case pipeline.NodeKindRetrieveMerge, pipeline.NodeKindCandidatesFetch, pipeline.NodeKindCandidatesModel, pipeline.NodeKindCandidatesMutate:
		next.Candidates = CloneCandidates(out.Candidates)
	}
	return next
}

func snapshotStep(node pipeline.CompiledNode, state State) (DebugStep, bool) {
	switch node.Kind {
	case pipeline.NodeKindContextFetch, pipeline.NodeKindContextModel, pipeline.NodeKindContextMutate:
		return SnapshotDebugStep(node.StageName, node.ID, node.Kind, DebugScopeContext, state), true
	case pipeline.NodeKindRetrieveMerge, pipeline.NodeKindCandidatesFetch, pipeline.NodeKindCandidatesModel, pipeline.NodeKindCandidatesMutate:
		return SnapshotDebugStep(node.StageName, node.ID, node.Kind, DebugScopeCandidates, state), true
	default:
		return DebugStep{}, false
	}
}

type stageTimer struct {
	Name      string
	Category  string
	StartedAt time.Time
}

func stageForGroup(group pipeline.ExecutionGroup, nodesByID map[string]pipeline.CompiledNode) *stageTimer {
	if len(group.NodeIDs) == 0 {
		return nil
	}
	node := nodesByID[group.NodeIDs[0]]
	category := metrics.StageCategoryForNodeKind(string(node.Kind))
	if category == "" {
		return nil
	}
	return &stageTimer{
		Name:      node.StageName,
		Category:  category,
		StartedAt: time.Now(),
	}
}

func nextStageForPlanGroup(groups []pipeline.ExecutionGroup, index int, nodesByID map[string]pipeline.CompiledNode) *stageTimer {
	if index >= len(groups) {
		return nil
	}
	return stageForGroup(groups[index], nodesByID)
}

func transitionStage(ctx context.Context, env *ExecEnv, pipelineName string, active, next *stageTimer, markOK bool) *stageTimer {
	if sameStage(active, next) {
		return active
	}
	if active != nil && markOK {
		recordStage(ctx, env, pipelineName, active, metrics.StatusOK)
	}
	if next == nil {
		return nil
	}
	return next
}

func transitionStageWithDuration(ctx context.Context, env *ExecEnv, pipelineName string, active, current *stageTimer, status string, _ time.Duration, force bool) *stageTimer {
	if force {
		if active != nil {
			recordStage(ctx, env, pipelineName, active, status)
		}
		return nil
	}
	return transitionStage(ctx, env, pipelineName, active, current, true)
}

func recordStage(ctx context.Context, env *ExecEnv, pipelineName string, stage *stageTimer, status string) {
	if stage == nil {
		return
	}
	env.MetricRecorder().RecordStage(ctx, metrics.StageMetric{
		Pipeline:      pipelineName,
		StageName:     stage.Name,
		StageCategory: stage.Category,
		Status:        status,
		Duration:      time.Since(stage.StartedAt),
	})
}

func sameStage(left, right *stageTimer) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.Name == right.Name && left.Category == right.Category
}

func nodeMetricStatus(err error) string {
	if err != nil {
		return metrics.StatusError
	}
	return metrics.StatusOK
}
