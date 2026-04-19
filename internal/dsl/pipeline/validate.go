package pipeline

import (
	"fmt"
	"reflect"

	"github.com/dmitryBe/weaver/internal/dsl/merge"
	"github.com/dmitryBe/weaver/internal/dsl/op"
	"github.com/dmitryBe/weaver/internal/dsl/policy"
	"github.com/dmitryBe/weaver/internal/dsl/retrieve"
)

type ValidationError struct {
	Path    string
	Message string
}

func (e ValidationError) Error() string {
	if e.Path == "" {
		return e.Message
	}
	return fmt.Sprintf("%s: %s", e.Path, e.Message)
}

func Validate(spec *Spec) error {
	if spec == nil {
		return ValidationError{Path: "pipeline", Message: "spec must not be nil"}
	}
	if spec.Name == "" {
		return ValidationError{Path: "pipeline.name", Message: "must not be empty"}
	}
	if len(spec.Stages) == 0 {
		return ValidationError{Path: "pipeline.stages", Message: "must contain at least one stage"}
	}
	if _, err := policy.ResolveResilience(spec.DefaultOptions); err != nil {
		return ValidationError{Path: "pipeline.defaults", Message: err.Error()}
	}

	seen := make(map[string]struct{}, len(spec.Stages))
	for i, stage := range spec.Stages {
		stagePath := fmt.Sprintf("pipeline.stages[%d]", i)
		if stage.Name == "" {
			return ValidationError{Path: stagePath + ".name", Message: "must not be empty"}
		}
		if _, exists := seen[stage.Name]; exists {
			return ValidationError{Path: stagePath + ".name", Message: fmt.Sprintf("duplicate stage name %q", stage.Name)}
		}
		seen[stage.Name] = struct{}{}

		switch stage.Kind {
		case StageContext, StageCandidates:
			if len(stage.Ops) == 0 {
				return ValidationError{Path: stagePath + ".ops", Message: "must contain at least one op"}
			}
			firstKind, err := validateHomogeneousOps(stage.Ops, stagePath+".ops")
			if err != nil {
				return err
			}
			if firstKind != op.KindFetch && firstKind != op.KindModel && firstKind != op.KindMutate {
				return ValidationError{Path: stagePath + ".ops", Message: fmt.Sprintf("unsupported op kind %q", firstKind)}
			}

		case StageRetrieve:
			if stage.Retrieve == nil {
				return ValidationError{Path: stagePath + ".retrieve", Message: "must not be nil"}
			}
			if err := validateRetrieveSpec(stage.Retrieve, stagePath+".retrieve"); err != nil {
				return err
			}

		default:
			return ValidationError{Path: stagePath + ".kind", Message: fmt.Sprintf("unsupported stage kind %q", stage.Kind)}
		}
	}

	return nil
}

func validateHomogeneousOps(ops []op.Op, path string) (op.Kind, error) {
	first := op.Kind("")
	for i, raw := range ops {
		if isNilOp(raw) {
			return "", ValidationError{Path: fmt.Sprintf("%s[%d]", path, i), Message: "must not be nil"}
		}
		kind := raw.Kind()
		if first == "" {
			first = kind
			continue
		}
		if kind != first {
			return "", ValidationError{
				Path:    fmt.Sprintf("%s[%d]", path, i),
				Message: fmt.Sprintf("mixed op kinds: %q and %q must be split into separate stages", first, kind),
			}
		}
	}
	return first, nil
}

func isNilOp(raw op.Op) bool {
	if raw == nil {
		return true
	}
	value := reflect.ValueOf(raw)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func validateRetrieveSpec(spec retrieve.Spec, path string) error {
	switch current := spec.(type) {
	case *retrieve.ParallelSpec:
		return validateParallelSpec(current, path)
	default:
		return ValidationError{Path: path, Message: "only retrieve.Parallel(...) is supported in Phase 2"}
	}
}

func validateParallelSpec(spec *retrieve.ParallelSpec, path string) error {
	if spec == nil {
		return ValidationError{Path: path, Message: "must not be nil"}
	}
	if len(spec.Sources) == 0 {
		return ValidationError{Path: path + ".sources", Message: "must contain at least one source"}
	}
	for i, source := range spec.Sources {
		if isNilSource(source) {
			return ValidationError{Path: fmt.Sprintf("%s.sources[%d]", path, i), Message: "must not be nil"}
		}
	}
	if spec.MergeSpec == (merge.Spec{}) {
		return ValidationError{Path: path + ".merge", Message: "must define a merge strategy"}
	}
	return nil
}

func isNilSource(source retrieve.Source) bool {
	if source == nil {
		return true
	}
	value := reflect.ValueOf(source)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
