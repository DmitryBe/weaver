package runtime

import (
	"context"
	"errors"
	"fmt"
)

type ErrorCategory string

const (
	CategoryInvalidArgument       ErrorCategory = "invalid_argument"
	CategoryDependencyFailed      ErrorCategory = "dependency_failed"
	CategoryDependencyTimeout     ErrorCategory = "dependency_timeout"
	CategoryPipelineMisconfigured ErrorCategory = "pipeline_misconfigured"
	CategoryDataUnavailable       ErrorCategory = "data_unavailable"
)

type ExecutionError struct {
	Category    ErrorCategory
	SafeMessage string
	Err         error
}

func (e *ExecutionError) Error() string {
	if e == nil {
		return ""
	}
	if e.Err == nil {
		return e.SafeMessage
	}
	if e.SafeMessage == "" {
		return e.Err.Error()
	}
	return fmt.Sprintf("%s: %v", e.SafeMessage, e.Err)
}

func (e *ExecutionError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func NewExecutionError(category ErrorCategory, safeMessage string, err error) error {
	return &ExecutionError{
		Category:    category,
		SafeMessage: safeMessage,
		Err:         err,
	}
}

func AsExecutionError(err error) (*ExecutionError, bool) {
	var target *ExecutionError
	if errors.As(err, &target) {
		return target, true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &ExecutionError{
			Category:    CategoryDependencyTimeout,
			SafeMessage: "upstream request timed out",
			Err:         err,
		}, true
	}
	return nil, false
}
