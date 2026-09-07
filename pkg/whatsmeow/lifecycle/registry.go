package lifecycle

import "sync"

// Registry protects every map access, including readers in other services.
// Pointer identity prevents an exiting generation from removing a successor.
type Registry[T comparable] struct {
	mu     sync.RWMutex
	values map[string]T
}

func (r *Registry[T]) Load(id string) T {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.values[id]
}

func (r *Registry[T]) Store(id string, value T) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.values == nil {
		r.values = make(map[string]T)
	}
	r.values[id] = value
}

func (r *Registry[T]) CompareDelete(id string, expected T) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if current, ok := r.values[id]; ok && current == expected {
		delete(r.values, id)
		return true
	}
	return false
}
