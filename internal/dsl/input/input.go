package input

type Kind string

const (
	KindString Kind = "string"
	KindInt    Kind = "int"
	KindFloat  Kind = "float"
)

type Field struct {
	Name string
	Kind Kind
}

func String(name string) Field {
	return Field{Name: name, Kind: KindString}
}

func Int(name string) Field {
	return Field{Name: name, Kind: KindInt}
}

func Float(name string) Field {
	return Field{Name: name, Kind: KindFloat}
}
