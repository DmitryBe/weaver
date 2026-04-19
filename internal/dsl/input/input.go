package input

type Kind string

const (
	KindString     Kind = "string"
	KindInt        Kind = "int"
	KindFloat      Kind = "float"
	KindCandidates Kind = "candidates"
)

type Field struct {
	Name   string
	Kind   Kind
	Fields []Field
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

func Candidates(fields ...Field) Field {
	return Field{
		Name:   "candidates",
		Kind:   KindCandidates,
		Fields: append([]Field(nil), fields...),
	}
}
