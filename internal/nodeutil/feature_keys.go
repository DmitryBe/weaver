package nodeutil

import (
	"fmt"
	"sort"
	"strings"

	"github.com/dmitryBe/weaver/internal/dsl/expr"
	"github.com/dmitryBe/weaver/internal/runtime"
)

func InferFeatureEntity(source string) (string, error) {
	entity, feature, ok := strings.Cut(source, "/")
	if !ok || entity == "" || feature == "" {
		return "", fmt.Errorf("feature %q must be in entity/name form", source)
	}
	return entity, nil
}

func BuildFeatureKey(source string, keyExpr expr.Expr, state runtime.State, candidate runtime.Candidate) (runtime.Key, error) {
	fields, err := inferFeatureKeyFields(source)
	if err != nil {
		return nil, err
	}
	values, err := resolveKeyValues(keyExpr, state, candidate)
	if err != nil {
		return nil, err
	}
	if len(fields) != len(values) {
		return nil, fmt.Errorf("feature %q expects %d key parts, got %d", source, len(fields), len(values))
	}

	key := make(runtime.Key, len(fields))
	for i, field := range fields {
		key[field] = values[i]
	}
	return key, nil
}

func MergeFeatureKeys(keys ...runtime.Key) (runtime.Key, error) {
	merged := make(runtime.Key)
	for _, current := range keys {
		for field, value := range current {
			if existing, ok := merged[field]; ok && existing != value {
				return nil, fmt.Errorf("conflicting values for key field %q: %q vs %q", field, existing, value)
			}
			merged[field] = value
		}
	}
	return merged, nil
}

func NormalizeKey(key runtime.Key) string {
	fields := make([]string, 0, len(key))
	for field := range key {
		fields = append(fields, field)
	}
	sort.Strings(fields)

	parts := make([]string, len(fields))
	for i, field := range fields {
		parts[i] = field + "=" + key[field]
	}
	return strings.Join(parts, "|")
}

func inferFeatureKeyFields(source string) ([]string, error) {
	entity, err := InferFeatureEntity(source)
	if err != nil {
		return nil, err
	}

	switch entity {
	case "user":
		return []string{"user_id"}, nil
	case "brand":
		return []string{"brand_id"}, nil
	case "brand_user":
		return []string{"brand_id", "user_id"}, nil
	default:
		return nil, fmt.Errorf("unsupported feature entity %q for %q", entity, source)
	}
}

func resolveKeyValues(current expr.Expr, state runtime.State, candidate runtime.Candidate) ([]string, error) {
	switch value := current.(type) {
	case expr.TupleExpr:
		out := make([]string, len(value.Parts))
		for i, part := range value.Parts {
			resolved, err := EvalExpr(part, state, candidate)
			if err != nil {
				return nil, err
			}
			out[i] = fmt.Sprint(resolved)
		}
		return out, nil
	default:
		resolved, err := EvalExpr(current, state, candidate)
		if err != nil {
			return nil, err
		}
		return []string{fmt.Sprint(resolved)}, nil
	}
}
