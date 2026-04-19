package merge

type Spec struct {
	DedupField     string
	SortScoreField string
	SortDescending bool
}

// Default concatenates candidates from multiple retrievers in source order.
// This is the default merge behavior (no sorting).
func Default() Spec {
	return Spec{}
}

// ByScore is kept for backwards compatibility. Prefer `Fuse().SortByScore(field)`.
func ByScore() Spec {
	return Spec{}.SortByScore("score")
}

func (s Spec) DedupBy(field string) Spec {
	s.DedupField = field
	return s
}

// SortByScore sorts merged candidates by a numeric-ish field in descending order.
// The field name must be provided explicitly to avoid relying on implicit defaults.
func (s Spec) SortByScore(field string) Spec {
	s.SortScoreField = field
	s.SortDescending = true
	return s
}
