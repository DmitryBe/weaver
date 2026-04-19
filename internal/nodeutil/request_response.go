package nodeutil

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/dmitryBe/weaver/internal/dsl/config"
	"github.com/dmitryBe/weaver/internal/dsl/expr"
	"github.com/dmitryBe/weaver/internal/dsl/expr/request"
	"github.com/dmitryBe/weaver/internal/dsl/expr/response"
	"github.com/dmitryBe/weaver/internal/runtime"
)

func BuildRequest(current request.Spec, state runtime.State, candidate runtime.Candidate) (any, error) {
	switch spec := current.(type) {
	case request.ObjectSpec:
		out := make(map[string]any, len(spec.Fields))
		for _, field := range spec.Fields {
			resolved, err := resolveRequestValue(field.Value, state, candidate)
			if err != nil {
				return nil, err
			}
			setPath(out, strings.Split(field.Path, "."), resolved)
		}
		return out, nil
	case request.ForEachCandidateSpec:
		items := make([]any, 0, len(state.Candidates))
		for _, currentCandidate := range state.Candidates {
			item, err := BuildRequest(spec.Item, state, currentCandidate)
			if err != nil {
				return nil, err
			}
			items = append(items, item)
		}
		return items, nil
	case request.JQSpec:
		return nil, fmt.Errorf("request.JQ is not implemented yet")
	default:
		return nil, fmt.Errorf("unsupported request spec %T", current)
	}
}

func ParseResponse(current response.Spec, raw any) (any, error) {
	switch spec := current.(type) {
	case response.PathSpec:
		if spec.Path == "" {
			return raw, nil
		}
		parsed, err := getValueAtPath(raw, strings.Split(spec.Path, "."))
		if err != nil {
			return nil, runtime.NewExecutionError(
				runtime.CategoryPipelineMisconfigured,
				"pipeline misconfigured: upstream response contract mismatch",
				err,
			)
		}
		return parsed, nil
	case response.JQSpec:
		return nil, fmt.Errorf("response.JQ is not implemented yet")
	default:
		return nil, fmt.Errorf("unsupported response spec %T", current)
	}
}

func NormalizeEndpoint(endpoint any) any {
	switch current := endpoint.(type) {
	case config.Value:
		return current.Literal
	default:
		return endpoint
	}
}

func resolveRequestValue(value any, state runtime.State, candidate runtime.Candidate) (any, error) {
	switch current := value.(type) {
	case expr.Expr:
		return EvalExpr(current, state, candidate)
	case request.Spec:
		return BuildRequest(current, state, candidate)
	default:
		return value, nil
	}
}

func getValueAtPath(root any, path []string) (any, error) {
	current := root
	for _, part := range path {
		switch typed := current.(type) {
		case map[string]any:
			next, ok := typed[part]
			if !ok {
				return nil, fmt.Errorf("missing response path %q", strings.Join(path, "."))
			}
			current = next
		case []any:
			index, err := strconv.Atoi(part)
			if err != nil {
				return nil, fmt.Errorf("response path %q cannot be resolved", strings.Join(path, "."))
			}
			if index < 0 || index >= len(typed) {
				return nil, fmt.Errorf("missing response path %q", strings.Join(path, "."))
			}
			current = typed[index]
		default:
			return nil, fmt.Errorf("response path %q cannot be resolved", strings.Join(path, "."))
		}
	}
	return current, nil
}
