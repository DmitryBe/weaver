package types

type Kind string

const (
	KindString Kind = "string"
	KindInt    Kind = "int"
	KindFloat  Kind = "float"
	KindArray  Kind = "array"
)

type Type struct {
	Kind Kind
	Elem *Type
}

var (
	String = Type{Kind: KindString}
	Int    = Type{Kind: KindInt}
	Float  = Type{Kind: KindFloat}
)

func Array(of Type) Type {
	elem := of
	return Type{
		Kind: KindArray,
		Elem: &elem,
	}
}
