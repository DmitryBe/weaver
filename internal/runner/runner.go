package runner

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/dmitryBe/weaver/internal/appconfig"
	"github.com/dmitryBe/weaver/internal/dsl/expr"
	"github.com/dmitryBe/weaver/internal/dsl/input"
	dslpipeline "github.com/dmitryBe/weaver/internal/dsl/pipeline"
	"github.com/dmitryBe/weaver/internal/nodes"
	"github.com/dmitryBe/weaver/internal/nodeutil"
	"github.com/dmitryBe/weaver/internal/runtime"
	_ "github.com/dmitryBe/weaver/pipelines"
)

type Runner struct {
	Config   *appconfig.AppConfig
	Registry *dslpipeline.Registry
	Engine   *runtime.Engine
	Env      *runtime.ExecEnv

	cleanup func()
}

type ExecuteRequest struct {
	Pipeline       string
	RequestID      string
	Context        map[string]any
	Candidates     []map[string]any
	NormalizeInput bool
	Debug          bool
}

type ExecuteResult struct {
	RequestID string
	Pipeline  string
	State     runtime.State
	CacheHit  bool
	DebugInfo *runtime.DebugInfo
}

type PipelineNotFoundError struct {
	Pipeline  string
	Available []string
}

func (e *PipelineNotFoundError) Error() string {
	return fmt.Sprintf("pipeline %q not found (available: %s)", e.Pipeline, strings.Join(e.Available, ", "))
}

type InvalidInputError struct {
	Message string
}

func (e *InvalidInputError) Error() string {
	return e.Message
}

func New(ctx context.Context, configPath string) (*Runner, error) {
	cfg, err := loadConfig(configPath)
	if err != nil {
		return nil, err
	}

	env, cleanup, err := appconfig.BuildExecEnv(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("build execution environment: %w", err)
	}

	if err := dslpipeline.DefaultRegistry.BuildAll(); err != nil {
		cleanup()
		return nil, fmt.Errorf("build pipeline registry: %w", err)
	}

	nodeRegistry := runtime.NewRegistry()
	nodes.RegisterDefaults(nodeRegistry)

	return &Runner{
		Config:   cfg,
		Registry: dslpipeline.DefaultRegistry,
		Engine:   runtime.NewEngine(nodeRegistry),
		Env:      env,
		cleanup:  cleanup,
	}, nil
}

func (r *Runner) Close() {
	if r == nil || r.cleanup == nil {
		return
	}
	r.cleanup()
	r.cleanup = nil
}

func (r *Runner) Execute(ctx context.Context, req ExecuteRequest) (ExecuteResult, error) {
	if r == nil {
		return ExecuteResult{}, fmt.Errorf("runner must not be nil")
	}
	if r.Registry == nil {
		return ExecuteResult{}, fmt.Errorf("pipeline registry must not be nil")
	}
	if r.Engine == nil {
		return ExecuteResult{}, fmt.Errorf("runtime engine must not be nil")
	}
	if r.Env == nil {
		return ExecuteResult{}, fmt.Errorf("execution environment must not be nil")
	}

	registered, ok := r.Registry.Get(req.Pipeline)
	if !ok {
		return ExecuteResult{}, &PipelineNotFoundError{
			Pipeline:  req.Pipeline,
			Available: listPipelineNames(r.Registry),
		}
	}

	contextFields := copyMap(req.Context)
	if req.NormalizeInput {
		contextFields = normalizeContextForInputs(registered.Spec, contextFields)
	}
	if err := validateInput(registered.Spec, contextFields); err != nil {
		return ExecuteResult{}, &InvalidInputError{Message: err.Error()}
	}

	requestID := req.RequestID
	if requestID == "" {
		requestID = newRequestID()
	}

	initial := runtime.State{
		Context:    runtime.Context(copyMap(contextFields)),
		Candidates: toCandidates(req.Candidates),
		Meta: runtime.Meta{
			RequestID: requestID,
			Pipeline:  registered.FullName,
		},
	}
	if initial.Context == nil {
		initial.Context = runtime.Context{}
	}

	if !req.Debug && registered.Spec.CacheSpec != nil && r.Env.Cache != nil {
		cacheKey, err := buildCacheKey(registered.FullName, registered.Spec.CacheSpec.KeyExpr, initial)
		if err != nil {
			return ExecuteResult{}, fmt.Errorf("build cache key for pipeline %q: %w", registered.FullName, err)
		}
		cached, ok, err := r.Env.Cache.Get(ctx, cacheKey)
		if err != nil {
			return ExecuteResult{}, fmt.Errorf("read cache for pipeline %q: %w", registered.FullName, err)
		}
		if ok {
			cached.Meta.RequestID = requestID
			if cached.Meta.Pipeline == "" {
				cached.Meta.Pipeline = registered.FullName
			}
			return ExecuteResult{
				RequestID: requestID,
				Pipeline:  registered.FullName,
				State:     cached,
				CacheHit:  true,
			}, nil
		}

		final, err := r.Engine.Run(ctx, registered.Plan, initial, r.Env)
		if err != nil {
			return ExecuteResult{}, fmt.Errorf("run pipeline %q: %w", registered.FullName, err)
		}
		ttl := registered.Spec.CacheSpec.TTLValue
		if ttl <= 0 {
			ttl = r.Env.CacheDefaultTTL
		}
		if err := r.Env.Cache.Set(ctx, cacheKey, final, ttl); err != nil {
			return ExecuteResult{}, fmt.Errorf("write cache for pipeline %q: %w", registered.FullName, err)
		}
		return ExecuteResult{
			RequestID: requestID,
			Pipeline:  registered.FullName,
			State:     final,
			CacheHit:  false,
		}, nil
	}

	runResult, err := r.Engine.RunWithOptions(ctx, registered.Plan, initial, r.Env, runtime.RunOptions{
		Debug: req.Debug,
	})
	if err != nil {
		return ExecuteResult{}, fmt.Errorf("run pipeline %q: %w", registered.FullName, err)
	}

	return ExecuteResult{
		RequestID: requestID,
		Pipeline:  registered.FullName,
		State:     runResult.State,
		CacheHit:  false,
		DebugInfo: runResult.DebugInfo,
	}, nil
}

func IsPipelineNotFound(err error) bool {
	var target *PipelineNotFoundError
	return errors.As(err, &target)
}

func IsInvalidInput(err error) bool {
	var target *InvalidInputError
	return errors.As(err, &target)
}

func loadConfig(path string) (*appconfig.AppConfig, error) {
	if strings.TrimSpace(path) != "" {
		return appconfig.LoadFromPath(path)
	}
	return appconfig.Load()
}

func validateInput(spec *dslpipeline.Spec, fields map[string]any) error {
	if spec == nil {
		return fmt.Errorf("pipeline spec must not be nil")
	}
	for _, current := range spec.Inputs {
		value, ok := fields[current.Name]
		if !ok {
			return fmt.Errorf("missing required input %q", current.Name)
		}
		if !matchesInputKind(current.Kind, value) {
			return fmt.Errorf("input %q must be %s, got %T", current.Name, current.Kind, value)
		}
	}
	return nil
}

func matchesInputKind(kind input.Kind, value any) bool {
	switch kind {
	case input.KindString:
		_, ok := value.(string)
		return ok
	case input.KindInt:
		switch value.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float64:
			return true
		default:
			return false
		}
	case input.KindFloat:
		switch value.(type) {
		case float32, float64, int, int8, int16, int32, int64:
			return true
		default:
			return false
		}
	default:
		return false
	}
}

func normalizeContextForInputs(spec *dslpipeline.Spec, fields map[string]any) map[string]any {
	normalized := copyMap(fields)
	if spec == nil {
		return normalized
	}

	for _, current := range spec.Inputs {
		value, ok := normalized[current.Name]
		if !ok {
			continue
		}
		normalized[current.Name] = coerceInputValue(current.Kind, value)
	}
	return normalized
}

func coerceInputValue(kind input.Kind, value any) any {
	raw, ok := value.(string)
	if !ok {
		return value
	}

	switch kind {
	case input.KindInt:
		parsed, err := strconv.Atoi(strings.TrimSpace(raw))
		if err == nil {
			return parsed
		}
	case input.KindFloat:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
		if err == nil {
			return parsed
		}
	}
	return value
}

func listPipelineNames(registry *dslpipeline.Registry) []string {
	registered := registry.List()
	names := make([]string, 0, len(registered))
	for _, current := range registered {
		names = append(names, current.FullName)
	}
	sort.Strings(names)
	return names
}

func buildCacheKey(pipelineName string, keyExpr expr.Expr, initial runtime.State) (string, error) {
	resolved, err := nodeutil.EvalExpr(keyExpr, initial, nil)
	if err != nil {
		return "", err
	}
	return pipelineName + ":" + fmt.Sprint(resolved), nil
}

func toCandidates(in []map[string]any) []runtime.Candidate {
	out := make([]runtime.Candidate, len(in))
	for i, candidate := range in {
		if candidate == nil {
			out[i] = nil
			continue
		}
		out[i] = runtime.Candidate(copyMap(candidate))
	}
	return out
}

func copyMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func newRequestID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return ""
	}
	return hex.EncodeToString(b)
}
