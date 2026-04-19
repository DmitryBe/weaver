package op

import (
	"github.com/dmitryBe/weaver/internal/dsl/expr"
	"github.com/dmitryBe/weaver/internal/dsl/types"
)

type SetOp struct {
	Field     string
	Value     expr.Expr
	FieldType *types.Type
}

func (*SetOp) isOp() {}

func (*SetOp) Kind() Kind {
	return KindMutate
}

func Set(field string, value expr.Expr) *SetOp {
	return &SetOp{
		Field: field,
		Value: value,
	}
}

func (o *SetOp) AsType(fieldType types.Type) *SetOp {
	copied := fieldType
	o.FieldType = &copied
	return o
}

func (o *SetOp) Float() *SetOp {
	return o.AsType(types.Float)
}

func (o *SetOp) Int() *SetOp {
	return o.AsType(types.Int)
}

func (o *SetOp) String() *SetOp {
	return o.AsType(types.String)
}

type DropOp struct {
	Field string
}

func (*DropOp) isOp() {}

func (*DropOp) Kind() Kind {
	return KindMutate
}

func Drop(field string) *DropOp {
	return &DropOp{Field: field}
}
