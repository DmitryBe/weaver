package nodeutil

import (
	"fmt"
	"sort"

	"github.com/dmitryBe/weaver/internal/runtime"
)

func DedupCandidatesByField(candidates []runtime.Candidate, field string) []runtime.Candidate {
	if field == "" {
		return candidates
	}

	seen := make(map[string]struct{}, len(candidates))
	out := make([]runtime.Candidate, 0, len(candidates))

	for _, c := range candidates {
		v, ok := c[field]
		if !ok {
			out = append(out, c)
			continue
		}

		key, ok := v.(string)
		if !ok {
			key = fmt.Sprint(v)
		}

		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, c)
	}

	return out
}

func SortCandidatesByField(candidates []runtime.Candidate, field string, descending bool) {
	sort.SliceStable(candidates, func(i, j int) bool {
		cmp := compareCandidateField(candidates[i][field], candidates[j][field])
		if descending {
			return cmp > 0
		}
		return cmp < 0
	})
}

func compareCandidateField(left, right any) int {
	if left == nil && right == nil {
		return 0
	}
	if left == nil {
		return -1
	}
	if right == nil {
		return 1
	}

	lf, lOk := toFloat(left)
	rf, rOk := toFloat(right)
	if lOk == nil && rOk == nil {
		switch {
		case lf < rf:
			return -1
		case lf > rf:
			return 1
		default:
			return 0
		}
	}

	ls := fmt.Sprint(left)
	rs := fmt.Sprint(right)
	switch {
	case ls < rs:
		return -1
	case ls > rs:
		return 1
	default:
		return 0
	}
}
