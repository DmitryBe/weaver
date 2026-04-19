package runtime

import (
	"context"
	"net/http"
	"reflect"
	"time"

	"github.com/dmitryBe/weaver/internal/dsl/pipeline"
	"github.com/dmitryBe/weaver/internal/metrics"
)

type Context map[string]any

type Candidate map[string]any

type Meta struct {
	RequestID string
	Pipeline  string
}

type State struct {
	Context    Context
	Candidates []Candidate
	Meta       Meta
}

type NodeInput struct {
	State     State
	Upstreams map[string]NodeOutput
}

type NodeOutput struct {
	Context    Context
	Candidates []Candidate
}

type DebugScope string

const (
	DebugScopeContext    DebugScope = "context"
	DebugScopeCandidates DebugScope = "candidates"
)

type DebugStep struct {
	StageName  string            `json:"stage_name"`
	NodeID     string            `json:"node_id"`
	NodeKind   pipeline.NodeKind `json:"node_kind"`
	Scope      DebugScope        `json:"scope"`
	Context    map[string]any    `json:"context"`
	Candidates []map[string]any  `json:"candidates"`
}

type DebugInfo struct {
	Steps []DebugStep `json:"steps"`
}

type RunOptions struct {
	Debug bool
}

type RunResult struct {
	State     State
	DebugInfo *DebugInfo
}

type Key = map[string]string

type FeatureStore interface {
	Fetch(ctx context.Context, features []string, keys ...Key) ([]map[string]any, error)
}

type KnowledgeGraph interface {
	Query(ctx context.Context, query string, params map[string]any) ([]map[string]any, error)
	Close(ctx context.Context) error
}

type Cache interface {
	Get(ctx context.Context, key string) (State, bool, error)
	Set(ctx context.Context, key string, value State, ttl time.Duration) error
}

type ExecEnv struct {
	FeatureStore                    FeatureStore
	FeatureStoreFetchMaxParallelism int
	Cache                           Cache
	CacheDefaultTTL                 time.Duration
	KnowledgeGraphs                 map[string]KnowledgeGraph
	Metrics                         metrics.Recorder
	MetricsHandler                  http.Handler
	MetricsPath                     string
}

func CloneContext(in Context) Context {
	if in == nil {
		return nil
	}
	out := make(Context, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func CloneCandidates(in []Candidate) []Candidate {
	if in == nil {
		return nil
	}
	out := make([]Candidate, len(in))
	for i, candidate := range in {
		if candidate == nil {
			out[i] = nil
			continue
		}
		cloned := make(Candidate, len(candidate))
		for k, v := range candidate {
			cloned[k] = v
		}
		out[i] = cloned
	}
	return out
}

func CloneNodeOutput(in NodeOutput) NodeOutput {
	return NodeOutput{
		Context:    CloneContext(in.Context),
		Candidates: CloneCandidates(in.Candidates),
	}
}

func CloneState(in State) State {
	return State{
		Context:    CloneContext(in.Context),
		Candidates: CloneCandidates(in.Candidates),
		Meta:       in.Meta,
	}
}

func DeepCloneContext(in Context) Context {
	if in == nil {
		return nil
	}
	out := make(Context, len(in))
	for k, v := range in {
		out[k] = DeepCloneValue(v)
	}
	return out
}

func DeepCloneCandidates(in []Candidate) []Candidate {
	if in == nil {
		return nil
	}
	out := make([]Candidate, len(in))
	for i, candidate := range in {
		if candidate == nil {
			out[i] = nil
			continue
		}
		cloned := make(Candidate, len(candidate))
		for k, v := range candidate {
			cloned[k] = DeepCloneValue(v)
		}
		out[i] = cloned
	}
	return out
}

func SnapshotDebugStep(stageName string, nodeID string, nodeKind pipeline.NodeKind, scope DebugScope, state State) DebugStep {
	return DebugStep{
		StageName:  stageName,
		NodeID:     nodeID,
		NodeKind:   nodeKind,
		Scope:      scope,
		Context:    map[string]any(DeepCloneContext(state.Context)),
		Candidates: candidatesToResponse(DeepCloneCandidates(state.Candidates)),
	}
}

func candidatesToResponse(in []Candidate) []map[string]any {
	out := make([]map[string]any, len(in))
	for i, candidate := range in {
		if candidate == nil {
			out[i] = nil
			continue
		}
		out[i] = map[string]any(candidate)
	}
	return out
}

func DeepCloneValue(in any) any {
	if in == nil {
		return nil
	}
	return deepCloneReflect(reflect.ValueOf(in)).Interface()
}

func deepCloneReflect(v reflect.Value) reflect.Value {
	if !v.IsValid() {
		return v
	}

	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		cloned := reflect.New(v.Type().Elem())
		cloned.Elem().Set(deepCloneReflect(v.Elem()))
		return cloned
	case reflect.Interface:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		cloned := deepCloneReflect(v.Elem())
		wrapped := reflect.New(v.Type()).Elem()
		wrapped.Set(cloned)
		return wrapped
	case reflect.Map:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		cloned := reflect.MakeMapWithSize(v.Type(), v.Len())
		iter := v.MapRange()
		for iter.Next() {
			cloned.SetMapIndex(iter.Key(), deepCloneReflect(iter.Value()))
		}
		return cloned
	case reflect.Slice:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		cloned := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		for i := 0; i < v.Len(); i++ {
			cloned.Index(i).Set(deepCloneReflect(v.Index(i)))
		}
		return cloned
	case reflect.Array:
		cloned := reflect.New(v.Type()).Elem()
		for i := 0; i < v.Len(); i++ {
			cloned.Index(i).Set(deepCloneReflect(v.Index(i)))
		}
		return cloned
	case reflect.Struct:
		cloned := reflect.New(v.Type()).Elem()
		for i := 0; i < v.NumField(); i++ {
			if cloned.Field(i).CanSet() {
				cloned.Field(i).Set(deepCloneReflect(v.Field(i)))
			}
		}
		return cloned
	default:
		return v
	}
}

func (e *ExecEnv) MetricRecorder() metrics.Recorder {
	if e == nil || e.Metrics == nil {
		return metrics.Noop()
	}
	return e.Metrics
}
