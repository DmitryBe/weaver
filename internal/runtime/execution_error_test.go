package runtime_test

import (
	"context"
	"errors"
	"testing"

	"github.com/dmitryBe/weaver/internal/runtime"
)

func TestAsExecutionErrorFindsWrappedExecutionError(t *testing.T) {
	err := errors.New("raw cause")
	wrapped := runtime.NewExecutionError(
		runtime.CategoryDataUnavailable,
		"required feature data unavailable for requested key",
		err,
	)

	got, ok := runtime.AsExecutionError(wrapped)
	if !ok {
		t.Fatal("expected execution error")
	}
	if got.Category != runtime.CategoryDataUnavailable {
		t.Fatalf("unexpected category: got %q", got.Category)
	}
	if got.SafeMessage != "required feature data unavailable for requested key" {
		t.Fatalf("unexpected safe message: got %q", got.SafeMessage)
	}
}

func TestAsExecutionErrorClassifiesDeadlineExceeded(t *testing.T) {
	got, ok := runtime.AsExecutionError(context.DeadlineExceeded)
	if !ok {
		t.Fatal("expected deadline exceeded to classify")
	}
	if got.Category != runtime.CategoryDependencyTimeout {
		t.Fatalf("unexpected category: got %q", got.Category)
	}
}
