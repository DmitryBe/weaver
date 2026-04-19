package op

type SortDir string

const (
	Asc  SortDir = "asc"
	Desc SortDir = "desc"
)

type SortByOp struct {
	Field string
	Dir   SortDir
}

func (*SortByOp) isOp() {}

func (*SortByOp) Kind() Kind {
	return KindMutate
}

func SortBy(field string, dir SortDir) *SortByOp {
	return &SortByOp{
		Field: field,
		Dir:   dir,
	}
}
