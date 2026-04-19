package expr

func Tuple(parts ...Expr) Expr {
	return TupleExpr{
		Parts: append([]Expr(nil), parts...),
	}
}
