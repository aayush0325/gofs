// Package keyedmutex provides per-key mutual exclusion with reference counting
// so entries are removed once no waiter holds them.
package keyedmutex

import "sync"

type entry struct {
	mu   sync.Mutex
	refs int
}

// Map is a concurrency-safe collection of named mutexes.
type Map struct {
	mu sync.Mutex
	m  map[string]*entry
}

// New returns an empty keyed mutex map.
func New() *Map {
	return &Map{m: make(map[string]*entry)}
}

// Lock acquires the mutex for key and returns its unlock function.
func (m *Map) Lock(key string) func() {
	m.mu.Lock()
	e := m.m[key]
	if e == nil {
		e = &entry{}
		m.m[key] = e
	}
	e.refs++
	m.mu.Unlock()

	e.mu.Lock()
	return func() {
		e.mu.Unlock()
		m.mu.Lock()
		e.refs--
		if e.refs == 0 {
			delete(m.m, key)
		}
		m.mu.Unlock()
	}
}
