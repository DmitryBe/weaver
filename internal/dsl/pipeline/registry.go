package pipeline

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
)

type Builder func() *Spec

type RegisteredPipeline struct {
	FullName string
	Domain   string
	Name     string
	Spec     *Spec
	Plan     *CompiledPlan
}

type registration struct {
	domain string
	name   string
	build  Builder
}

type Registry struct {
	mu            sync.RWMutex
	registrations []registration
	built         map[string]RegisteredPipeline
}

var DefaultRegistry = NewRegistry()

func NewRegistry() *Registry {
	return &Registry{
		built: make(map[string]RegisteredPipeline),
	}
}

func (r *Registry) Register(domain string, name string, build Builder) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.registrations = append(r.registrations, registration{
		domain: domain,
		name:   name,
		build:  build,
	})
}

func (r *Registry) BuildAll() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	nextBuilt := make(map[string]RegisteredPipeline, len(r.registrations))
	seen := make(map[string]struct{}, len(r.registrations))
	ordered := append([]registration(nil), r.registrations...)
	sort.Slice(ordered, func(i, j int) bool {
		return fullNameFor(ordered[i].domain, ordered[i].name) < fullNameFor(ordered[j].domain, ordered[j].name)
	})

	for _, current := range ordered {
		if current.domain == "" {
			return fmt.Errorf("pipeline domain must not be empty")
		}
		if current.name == "" {
			return fmt.Errorf("pipeline name must not be empty")
		}
		if strings.Contains(current.domain, ".") {
			return fmt.Errorf("pipeline domain %q must not contain '.'", current.domain)
		}
		if strings.Contains(current.name, ".") {
			return fmt.Errorf("pipeline name %q must not contain '.'", current.name)
		}
		if current.build == nil {
			return fmt.Errorf("pipeline %q builder must not be nil", fullNameFor(current.domain, current.name))
		}

		fullName := fullNameFor(current.domain, current.name)
		if _, exists := seen[fullName]; exists {
			return fmt.Errorf("duplicate pipeline registration %q", fullName)
		}
		seen[fullName] = struct{}{}

		spec := current.build()
		if spec == nil {
			return fmt.Errorf("pipeline %q builder returned nil spec", fullName)
		}
		if spec.Name == "" {
			spec.Name = fullName
		} else if spec.Name != fullName {
			return fmt.Errorf("pipeline %q builder returned spec named %q", fullName, spec.Name)
		}

		plan, err := Compile(spec)
		if err != nil {
			return fmt.Errorf("compile pipeline %q: %w", fullName, err)
		}

		nextBuilt[fullName] = RegisteredPipeline{
			FullName: fullName,
			Domain:   current.domain,
			Name:     current.name,
			Spec:     spec,
			Plan:     plan,
		}

		log.Printf("[LOADED] pipeline=%s", fullName)
	}

	r.built = nextBuilt
	return nil
}

func (r *Registry) Get(fullName string) (RegisteredPipeline, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	registered, ok := r.built[fullName]
	return registered, ok
}

func (r *Registry) List() []RegisteredPipeline {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.built))
	for name := range r.built {
		names = append(names, name)
	}
	sort.Strings(names)

	out := make([]RegisteredPipeline, 0, len(names))
	for _, name := range names {
		out = append(out, r.built[name])
	}
	return out
}

func fullNameFor(domain string, name string) string {
	return domain + "." + name
}
