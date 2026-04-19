package nodeutil

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/dmitryBe/weaver/internal/dsl/expr"
	"github.com/dmitryBe/weaver/internal/runtime"
)

func EvalExpr(current expr.Expr, state runtime.State, candidate runtime.Candidate) (any, error) {
	switch value := current.(type) {
	case expr.RefExpr:
		switch value.Scope {
		case expr.ScopeContext:
			resolved, err := GetPath(state.Context, value.Path)
			if err != nil {
				return nil, runtime.NewExecutionError(
					runtime.CategoryInvalidArgument,
					fmt.Sprintf("invalid request context: missing field %q", strings.Join(value.Path, ".")),
					err,
				)
			}
			return resolved, nil
		case expr.ScopeCandidate:
			if candidate == nil {
				return nil, fmt.Errorf("candidate expression %v requires candidate scope", value.Path)
			}
			resolved, err := GetPath(candidate, value.Path)
			if err != nil {
				return nil, runtime.NewExecutionError(
					runtime.CategoryInvalidArgument,
					fmt.Sprintf("invalid request candidates: missing field %q", strings.Join(value.Path, ".")),
					err,
				)
			}
			return resolved, nil
		default:
			return nil, fmt.Errorf("unsupported ref scope %q", value.Scope)
		}
	case expr.ConstExpr:
		return value.Value, nil
	case expr.TupleExpr:
		parts := make([]string, len(value.Parts))
		for i, part := range value.Parts {
			resolved, err := EvalExpr(part, state, candidate)
			if err != nil {
				return nil, err
			}
			parts[i] = fmt.Sprint(resolved)
		}
		return strings.Join(parts, "|"), nil
	case expr.PadLeftExpr:
		resolved, err := EvalExpr(value.Value, state, candidate)
		if err != nil {
			return nil, err
		}
		items, err := toAnySlice(resolved)
		if err != nil {
			return nil, err
		}
		if len(items) >= value.Size {
			return items, nil
		}
		padded := make([]any, 0, value.Size)
		for i := 0; i < value.Size-len(items); i++ {
			padded = append(padded, value.Pad)
		}
		padded = append(padded, items...)
		return padded, nil
	default:
		return nil, fmt.Errorf("unsupported expression type %T", current)
	}
}

func GetPath(root any, path []string) (any, error) {
	var current any = root
	for _, part := range path {
		asMap, ok := asStringAnyMap(current)
		if !ok {
			return nil, fmt.Errorf("path %q cannot be resolved", strings.Join(path, "."))
		}
		next, ok := asMap[part]
		if !ok {
			return nil, fmt.Errorf("missing path %q", strings.Join(path, "."))
		}
		current = next
	}
	return current, nil
}

func asStringAnyMap(value any) (map[string]any, bool) {
	switch current := value.(type) {
	case map[string]any:
		return current, true
	case runtime.Context:
		return map[string]any(current), true
	case runtime.Candidate:
		return map[string]any(current), true
	default:
		return nil, false
	}
}

func setPath(root map[string]any, path []string, value any) {
	current := root
	for i, part := range path {
		if i == len(path)-1 {
			current[part] = value
			return
		}
		next, ok := current[part].(map[string]any)
		if !ok {
			next = map[string]any{}
			current[part] = next
		}
		current = next
	}
}

func toAnySlice(value any) ([]any, error) {
	if value == nil {
		return nil, nil
	}
	if current, ok := value.([]any); ok {
		return append([]any(nil), current...), nil
	}
	raw := reflect.ValueOf(value)
	if raw.Kind() != reflect.Slice && raw.Kind() != reflect.Array {
		return nil, fmt.Errorf("value of type %T is not a slice", value)
	}
	items := make([]any, raw.Len())
	for i := 0; i < raw.Len(); i++ {
		items[i] = raw.Index(i).Interface()
	}
	return items, nil
}
