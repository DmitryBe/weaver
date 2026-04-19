package retrieve

import "github.com/dmitryBe/weaver/internal/dsl/param"

type KGSpec struct {
	QueryPath   string
	Bindings    []param.Binding
	ClusterName string
	Limit       int
}

func (*KGSpec) isRetrieveSpec() {}

func (*KGSpec) isSource() {}

func (*KGSpec) SourceType() string {
	return "kg"
}

func KG(queryPath string) *KGSpec {
	return &KGSpec{QueryPath: queryPath}
}

func (s *KGSpec) Params(bindings ...param.Binding) *KGSpec {
	s.Bindings = append([]param.Binding(nil), bindings...)
	return s
}

func (s *KGSpec) Cluster(name string) *KGSpec {
	s.ClusterName = name
	return s
}

func (s *KGSpec) TopK(limit int) *KGSpec {
	s.Limit = limit
	return s
}
