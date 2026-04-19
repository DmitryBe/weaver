package expr

type Expr interface {
	isExpr()
}

type Scope string

const (
	ScopeContext   Scope = "context"
	ScopeCandidate Scope = "candidate"
	ScopeConst     Scope = "const"
	ScopeComposite Scope = "composite"
)

type RefExpr struct {
	Scope Scope
	Path  []string
}

func (RefExpr) isExpr() {}

type ConstExpr struct {
	Value any
}

func (ConstExpr) isExpr() {}

type TupleExpr struct {
	Parts []Expr
}

func (TupleExpr) isExpr() {}

type PadLeftExpr struct {
	Value Expr
	Size  int
	Pad   any
}

func (PadLeftExpr) isExpr() {}
