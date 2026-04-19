package response

type Spec interface {
	isResponseSpec()
}

type PathSpec struct {
	Path string
}

func (PathSpec) isResponseSpec() {}

type JQSpec struct {
	Source string
}

func (JQSpec) isResponseSpec() {}

func Path(path string) Spec {
	return PathSpec{Path: path}
}

func JQ(src string) Spec {
	return JQSpec{Source: src}
}

func Scores() Spec {
	return Path("scores")
}
