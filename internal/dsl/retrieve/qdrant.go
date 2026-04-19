package retrieve

type QdrantSpec struct {
	Index string
	Limit int
}

func (*QdrantSpec) isRetrieveSpec() {}

func (*QdrantSpec) isSource() {}

func (*QdrantSpec) SourceType() string {
	return "qdrant"
}

func Qdrant(index string) *QdrantSpec {
	return &QdrantSpec{Index: index}
}

func (s *QdrantSpec) TopK(limit int) *QdrantSpec {
	s.Limit = limit
	return s
}
