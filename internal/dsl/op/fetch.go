package op

import (
	"github.com/dmitryBe/weaver/internal/dsl/expr"
	"github.com/dmitryBe/weaver/internal/dsl/types"
)

type Kind string

const (
	KindFetch  Kind = "fetch"
	KindModel  Kind = "model"
	KindMutate Kind = "mutate"
)

type Op interface {
	isOp()
	Kind() Kind
}

type FetchOp struct {
	Source       string
	KeyExpr      expr.Expr
	IntoField    string
	DefaultValue any
	FieldType    *types.Type
}

func (*FetchOp) isOp() {}

func (*FetchOp) Kind() Kind {
	return KindFetch
}

func Fetch(source string) *FetchOp {
	return &FetchOp{Source: source}
}

func (o *FetchOp) Key(key expr.Expr) *FetchOp {
	o.KeyExpr = key
	return o
}

func (o *FetchOp) Into(field string) *FetchOp {
	o.IntoField = field
	return o
}

func (o *FetchOp) Default(value any) *FetchOp {
	o.DefaultValue = value
	return o
}

func (o *FetchOp) AsType(fieldType types.Type) *FetchOp {
	copied := fieldType
	o.FieldType = &copied
	return o
}

func (o *FetchOp) Float() *FetchOp {
	return o.AsType(types.Float)
}

func (o *FetchOp) Int() *FetchOp {
	return o.AsType(types.Int)
}

func (o *FetchOp) String() *FetchOp {
	return o.AsType(types.String)
}
