package job

import (
	"fmt"
	"sync"
)

// Registry maps job type names to their handlers.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]Handler)}
}

func (r *Registry) Register(jobType string, h Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[jobType] = h
}

func (r *Registry) Get(jobType string) (Handler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[jobType]
	return h, ok
}

func (r *Registry) Types() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	types := make([]string, 0, len(r.handlers))
	for t := range r.handlers {
		types = append(types, t)
	}
	return types
}

// Has reports whether a type is known, used by the API to validate
// CreateJob requests without executing anything.
func (r *Registry) Has(jobType string) bool {
	_, ok := r.Get(jobType)
	return ok
}

var ErrUnknownJobType = fmt.Errorf("unknown job type")
