package param

type Kind string

const (
	KindString Kind = "string"
	KindInt    Kind = "int"
)

type Binding struct {
	Name  string
	Kind  Kind
	Value any
}

func String(name string, value any) Binding {
	return Binding{
		Name:  name,
		Kind:  KindString,
		Value: value,
	}
}

func Int(name string, value any) Binding {
	return Binding{
		Name:  name,
		Kind:  KindInt,
		Value: value,
	}
}
