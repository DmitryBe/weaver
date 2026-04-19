package pipeline_test

import (
	"testing"

	"github.com/dmitryBe/weaver/internal/dsl/input"
	"github.com/dmitryBe/weaver/internal/dsl/op"
	"github.com/dmitryBe/weaver/internal/dsl/pipeline"
)

func TestRegistryBuildAllSuccess(t *testing.T) {
	registry := pipeline.NewRegistry()
	registry.Register("suite", "alpha", func() *pipeline.Spec {
		return pipeline.New().Candidates("post_rank", op.Take(1))
	})

	if err := registry.BuildAll(); err != nil {
		t.Fatalf("BuildAll failed: %v", err)
	}

	registered, ok := registry.Get("suite.alpha")
	if !ok {
		t.Fatal("expected registry entry")
	}
	if registered.Spec == nil {
		t.Fatal("expected built spec")
	}
	if registered.Plan == nil {
		t.Fatal("expected compiled plan")
	}
}

func TestRegistryBuildAllFailsOnDuplicateRegistration(t *testing.T) {
	registry := pipeline.NewRegistry()
	builder := func() *pipeline.Spec {
		return pipeline.New().Candidates("post_rank", op.Take(1))
	}

	registry.Register("suite", "alpha", builder)
	registry.Register("suite", "alpha", builder)

	if err := registry.BuildAll(); err == nil {
		t.Fatal("expected duplicate registration error")
	}
}

func TestRegistryBuildAllFailsOnNilBuilder(t *testing.T) {
	registry := pipeline.NewRegistry()
	registry.Register("suite", "alpha", nil)

	if err := registry.BuildAll(); err == nil {
		t.Fatal("expected nil builder error")
	}
}

func TestRegistryBuildAllFailsOnNilSpec(t *testing.T) {
	registry := pipeline.NewRegistry()
	registry.Register("suite", "alpha", func() *pipeline.Spec { return nil })

	if err := registry.BuildAll(); err == nil {
		t.Fatal("expected nil spec error")
	}
}

func TestRegistryBuildAllFailsOnCompileError(t *testing.T) {
	registry := pipeline.NewRegistry()
	registry.Register("suite", "bad", func() *pipeline.Spec {
		return pipeline.New().Input(input.String("city"))
	})

	if err := registry.BuildAll(); err == nil {
		t.Fatal("expected compile error")
	}
}

func TestRegistryBuildAllFailsOnSpecNameMismatch(t *testing.T) {
	registry := pipeline.NewRegistry()
	registry.Register("suite", "alpha", func() *pipeline.Spec {
		return pipeline.New("suite.beta").Candidates("post_rank", op.Take(1))
	})

	if err := registry.BuildAll(); err == nil {
		t.Fatal("expected spec name mismatch error")
	}
}

func TestRegistryListIsDeterministic(t *testing.T) {
	registry := pipeline.NewRegistry()
	registry.Register("suite", "bravo", func() *pipeline.Spec {
		return pipeline.New().Candidates("post_rank", op.Take(1))
	})
	registry.Register("suite", "alpha", func() *pipeline.Spec {
		return pipeline.New().Candidates("post_rank", op.Take(1))
	})

	if err := registry.BuildAll(); err != nil {
		t.Fatalf("BuildAll failed: %v", err)
	}

	listed := registry.List()
	if got, want := len(listed), 2; got != want {
		t.Fatalf("unexpected list size: got %d want %d", got, want)
	}
	if got, want := listed[0].FullName, "suite.alpha"; got != want {
		t.Fatalf("unexpected first full name: got %q want %q", got, want)
	}
	if got, want := listed[1].FullName, "suite.bravo"; got != want {
		t.Fatalf("unexpected second full name: got %q want %q", got, want)
	}
}
