package retrieve

import "github.com/dmitryBe/weaver/internal/dsl/expr"

type KVSpec struct {
	Feature string
	KeyExpr expr.Expr
	Limit   int
}

func (*KVSpec) isRetrieveSpec() {}

func (*KVSpec) isSource() {}

func (*KVSpec) SourceType() string {
	return "kv"
}

func KV(feature string) *KVSpec {
	return &KVSpec{Feature: feature}
}

func (s *KVSpec) Key(key expr.Expr) *KVSpec {
	s.KeyExpr = key
	return s
}

func (s *KVSpec) TopK(limit int) *KVSpec {
	s.Limit = limit
	return s
}
