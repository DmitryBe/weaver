package request

type JQSpec struct {
	Source string
}

func (JQSpec) isRequestSpec() {}

func JQ(src string) Spec {
	return JQSpec{Source: src}
}
