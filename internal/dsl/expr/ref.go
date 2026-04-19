package expr

func Context(path ...string) Expr {
	return RefExpr{
		Scope: ScopeContext,
		Path:  append([]string(nil), path...),
	}
}

func Candidate(path ...string) Expr {
	return RefExpr{
		Scope: ScopeCandidate,
		Path:  append([]string(nil), path...),
	}
}

func Const(value any) Expr {
	return ConstExpr{Value: value}
}

func PadLeft(value Expr, size int, pad any) Expr {
	return PadLeftExpr{
		Value: value,
		Size:  size,
		Pad:   pad,
	}
}
