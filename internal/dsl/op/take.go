package op

type TakeOp struct {
	Limit int
}

func (*TakeOp) isOp() {}

func (*TakeOp) Kind() Kind {
	return KindMutate
}

func Take(limit int) *TakeOp {
	return &TakeOp{Limit: limit}
}
