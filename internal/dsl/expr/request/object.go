package request

type Spec interface {
	isRequestSpec()
}

type FieldSpec struct {
	Path  string
	Value any
}

type ObjectSpec struct {
	Fields []FieldSpec
}

func (ObjectSpec) isRequestSpec() {}

type ForEachCandidateSpec struct {
	Item Spec
}

func (ForEachCandidateSpec) isRequestSpec() {}

func Field(path string, value any) FieldSpec {
	return FieldSpec{
		Path:  path,
		Value: value,
	}
}

func Object(fields ...FieldSpec) Spec {
	return ObjectSpec{
		Fields: append([]FieldSpec(nil), fields...),
	}
}

func ForEachCandidate(spec Spec) Spec {
	return ForEachCandidateSpec{Item: spec}
}
