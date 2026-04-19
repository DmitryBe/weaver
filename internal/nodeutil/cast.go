package nodeutil

import (
	"fmt"
	"strconv"

	"github.com/dmitryBe/weaver/internal/dsl/types"
	"github.com/dmitryBe/weaver/internal/runtime"
)

func CastValue(value any, fieldType *types.Type) (any, error) {
	if fieldType == nil {
		return value, nil
	}

	switch fieldType.Kind {
	case types.KindString:
		return fmt.Sprint(value), nil
	case types.KindInt:
		return toInt(value)
	case types.KindFloat:
		return toFloat(value)
	case types.KindArray:
		items, err := toAnySlice(value)
		if err != nil {
			return nil, err
		}
		if fieldType.Elem == nil {
			return items, nil
		}
		switch fieldType.Elem.Kind {
		case types.KindInt:
			out := make([]int, len(items))
			for i, item := range items {
				converted, err := toInt(item)
				if err != nil {
					return nil, err
				}
				out[i] = converted
			}
			return out, nil
		case types.KindFloat:
			out := make([]float64, len(items))
			for i, item := range items {
				converted, err := toFloat(item)
				if err != nil {
					return nil, err
				}
				out[i] = converted
			}
			return out, nil
		case types.KindString:
			out := make([]string, len(items))
			for i, item := range items {
				out[i] = fmt.Sprint(item)
			}
			return out, nil
		default:
			return items, nil
		}
	default:
		return value, nil
	}
}

func ToScoreSlice(value any) ([]any, error) {
	scores, err := toAnySlice(value)
	if err != nil {
		return nil, runtime.NewExecutionError(
			runtime.CategoryPipelineMisconfigured,
			"pipeline misconfigured: upstream response contract mismatch",
			err,
		)
	}
	return scores, nil
}

func toInt(value any) (int, error) {
	switch current := value.(type) {
	case int:
		return current, nil
	case int8:
		return int(current), nil
	case int16:
		return int(current), nil
	case int32:
		return int(current), nil
	case int64:
		return int(current), nil
	case uint:
		return int(current), nil
	case uint8:
		return int(current), nil
	case uint16:
		return int(current), nil
	case uint32:
		return int(current), nil
	case uint64:
		return int(current), nil
	case float32:
		return int(current), nil
	case float64:
		return int(current), nil
	case string:
		parsed, err := strconv.Atoi(current)
		if err != nil {
			return 0, err
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int", value)
	}
}

func toFloat(value any) (float64, error) {
	switch current := value.(type) {
	case float64:
		return current, nil
	case float32:
		return float64(current), nil
	case int:
		return float64(current), nil
	case int8:
		return float64(current), nil
	case int16:
		return float64(current), nil
	case int32:
		return float64(current), nil
	case int64:
		return float64(current), nil
	case uint:
		return float64(current), nil
	case uint8:
		return float64(current), nil
	case uint16:
		return float64(current), nil
	case uint32:
		return float64(current), nil
	case uint64:
		return float64(current), nil
	case string:
		parsed, err := strconv.ParseFloat(current, 64)
		if err != nil {
			return 0, err
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float", value)
	}
}
