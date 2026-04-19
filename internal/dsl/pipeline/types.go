package pipeline

import (
	"github.com/dmitryBe/weaver/internal/dsl/cache"
	"github.com/dmitryBe/weaver/internal/dsl/input"
	"github.com/dmitryBe/weaver/internal/dsl/op"
	"github.com/dmitryBe/weaver/internal/dsl/policy"
	"github.com/dmitryBe/weaver/internal/dsl/retrieve"
)

type StageKind string

const (
	StageContext    StageKind = "context"
	StageRetrieve   StageKind = "retrieve"
	StageCandidates StageKind = "candidates"
)

type Stage struct {
	Name     string
	Kind     StageKind
	Ops      []op.Op
	Retrieve retrieve.Spec
}

type Spec struct {
	Name           string
	Inputs         []input.Field
	DefaultOptions []policy.Option
	CacheSpec      *cache.Config
	Stages         []Stage
}
