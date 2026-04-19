package op

import (
	"github.com/dmitryBe/weaver/internal/dsl/expr/request"
	"github.com/dmitryBe/weaver/internal/dsl/expr/response"
)

type ModelOp struct {
	Name         string
	EndpointValue any
	RequestSpec  request.Spec
	ResponseSpec response.Spec
	IntoField    string
}

func (*ModelOp) isOp() {}

func (*ModelOp) Kind() Kind {
	return KindModel
}

func Model(name string) *ModelOp {
	return &ModelOp{Name: name}
}

func (o *ModelOp) Endpoint(value any) *ModelOp {
	o.EndpointValue = value
	return o
}

func (o *ModelOp) Request(spec request.Spec) *ModelOp {
	o.RequestSpec = spec
	return o
}

func (o *ModelOp) Response(spec response.Spec) *ModelOp {
	o.ResponseSpec = spec
	return o
}

func (o *ModelOp) Into(field string) *ModelOp {
	o.IntoField = field
	return o
}
