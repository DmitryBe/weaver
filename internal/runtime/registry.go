package runtime

import (
	"fmt"

	"github.com/dmitryBe/weaver/internal/dsl/pipeline"
)

type Registry struct {
	executors map[pipeline.NodeKind]NodeExecutor
}

func NewRegistry() *Registry {
	return &Registry{
		executors: make(map[pipeline.NodeKind]NodeExecutor),
	}
}

func (r *Registry) Register(kind pipeline.NodeKind, executor NodeExecutor) {
	r.executors[kind] = executor
}

func (r *Registry) ExecutorFor(kind pipeline.NodeKind) (NodeExecutor, error) {
	executor, ok := r.executors[kind]
	if !ok {
		return nil, fmt.Errorf("no executor registered for node kind %q", kind)
	}
	return executor, nil
}
