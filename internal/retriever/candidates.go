package retriever

import (
	"fmt"
	"reflect"

	"github.com/dmitryBe/weaver/internal/runtime"
)

func decodeCandidatesFromValue(value any) ([]runtime.Candidate, error) {
	if value == nil {
		return nil, fmt.Errorf("stored value must be an array of objects, got nil")
	}

	items, ok := value.([]any)
	if !ok {
		raw := reflect.ValueOf(value)
		if raw.Kind() != reflect.Slice && raw.Kind() != reflect.Array {
			return nil, fmt.Errorf("stored value must be an array of objects, got %T", value)
		}
		items = make([]any, raw.Len())
		for i := 0; i < raw.Len(); i++ {
			items[i] = raw.Index(i).Interface()
		}
	}

	out := make([]runtime.Candidate, 0, len(items))
	for i, item := range items {
		candidate, err := candidateFromObject(item, i)
		if err != nil {
			return nil, err
		}
		out = append(out, candidate)
	}
	return out, nil
}

func decodeCandidatesFromRows(rows []map[string]any) ([]runtime.Candidate, error) {
	out := make([]runtime.Candidate, 0, len(rows))
	for i, row := range rows {
		candidate, err := candidateFromObject(row, i)
		if err != nil {
			return nil, err
		}
		out = append(out, candidate)
	}
	return out, nil
}

func candidateFromObject(item any, index int) (runtime.Candidate, error) {
	obj, ok := item.(map[string]any)
	if !ok {
		if cand, ok := item.(runtime.Candidate); ok {
			obj = map[string]any(cand)
		} else {
			return nil, fmt.Errorf("stored candidate at index %d must be an object, got %T", index, item)
		}
	}

	candidate := make(runtime.Candidate, len(obj))
	for k, v := range obj {
		candidate[k] = v
	}
	if _, ok := candidate["brand_id"]; !ok || candidate["brand_id"] == nil {
		return nil, fmt.Errorf("candidate at index %d missing required field %q", index, "brand_id")
	}
	if _, ok := candidate["score"]; !ok || candidate["score"] == nil {
		return nil, fmt.Errorf("candidate at index %d missing required field %q", index, "score")
	}
	return candidate, nil
}
