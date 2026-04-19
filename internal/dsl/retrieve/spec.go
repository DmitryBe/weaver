package retrieve

import "github.com/dmitryBe/weaver/internal/dsl/merge"

type Spec interface {
	isRetrieveSpec()
}

type Source interface {
	Spec
	isSource()
	SourceType() string
}

type ParallelSpec struct {
	Sources   []Source
	MergeSpec merge.Spec
}

func (*ParallelSpec) isRetrieveSpec() {}

func Parallel(sources ...Source) *ParallelSpec {
	return &ParallelSpec{
		Sources: append([]Source(nil), sources...),
	}
}

func (s *ParallelSpec) Merge(spec merge.Spec) *ParallelSpec {
	s.MergeSpec = spec
	return s
}
