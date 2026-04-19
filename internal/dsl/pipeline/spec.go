package pipeline

import (
	"github.com/dmitryBe/weaver/internal/dsl/cache"
	"github.com/dmitryBe/weaver/internal/dsl/input"
	"github.com/dmitryBe/weaver/internal/dsl/op"
	"github.com/dmitryBe/weaver/internal/dsl/policy"
	"github.com/dmitryBe/weaver/internal/dsl/resilient"
	"github.com/dmitryBe/weaver/internal/dsl/retrieve"
)

func New(name ...string) *Spec {
	spec := &Spec{}
	if len(name) > 0 {
		spec.Name = name[0]
	}
	return spec
}

func (s *Spec) Input(fields ...input.Field) *Spec {
	s.Inputs = append(s.Inputs, fields...)
	return s
}

func (s *Spec) Defaults(options ...policy.Option) *Spec {
	s.DefaultOptions = append(s.DefaultOptions, options...)
	return s
}

func (s *Spec) Resilience(cfg resilient.Config) *Spec {
	return s.Defaults(policy.Resilience(cfg))
}

func (s *Spec) Cache(cfg *cache.Config) *Spec {
	s.CacheSpec = cfg
	return s
}

func (s *Spec) Context(name string, stageOps ...op.Op) *Spec {
	s.Stages = append(s.Stages, Stage{
		Name: name,
		Kind: StageContext,
		Ops:  append([]op.Op(nil), stageOps...),
	})
	return s
}

func (s *Spec) Retrieve(name string, spec retrieve.Spec) *Spec {
	s.Stages = append(s.Stages, Stage{
		Name:     name,
		Kind:     StageRetrieve,
		Retrieve: spec,
	})
	return s
}

func (s *Spec) Candidates(name string, stageOps ...op.Op) *Spec {
	s.Stages = append(s.Stages, Stage{
		Name: name,
		Kind: StageCandidates,
		Ops:  append([]op.Op(nil), stageOps...),
	})
	return s
}
