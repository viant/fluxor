package store

import (
	"context"
	"sync"

	"github.com/viant/fluxor/service/dao"
)

// MemoryStore is a generic in-memory implementation of dao.Service.
// It keeps entities of type *T mapped by a comparable key K.
// The key is obtained from the supplied keySelector function.
//
// This helper lets concrete DAOs embed the store and avoid
// rewriting identical Save/Load/Delete/List logic for every entity type.
//
// It purposefully contains no extra business logic such as state-based
// filtering – higher-level DAOs can still override List if they need
// additional behaviour.
type MemoryStore[K comparable, T any] struct {
	mu          sync.RWMutex
	records     map[K]*T
	keySelector func(*T) K
}

// NewMemoryStore creates a new MemoryStore.
// keySelector extracts the entity key (usually the ID field) from a value.
func NewMemoryStore[K comparable, T any](keySelector func(*T) K) *MemoryStore[K, T] {
	return &MemoryStore[K, T]{
		records:     make(map[K]*T),
		keySelector: keySelector,
	}
}

// Save stores or overwrites a record.
func (s *MemoryStore[K, T]) Save(_ context.Context, v *T) error {
	if v == nil {
		return nil // silently ignore – callers validate beforehand
	}
	key := s.keySelector(v)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[key] = v
	return nil
}

// Load returns a record by key.
func (s *MemoryStore[K, T]) Load(_ context.Context, key K) (*T, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.records[key]
	if !ok {
		return nil, nil
	}
	return v, nil
}

// Delete removes a record.
func (s *MemoryStore[K, T]) Delete(_ context.Context, key K) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, key)
	return nil
}

// List returns all stored records.
func (s *MemoryStore[K, T]) List(_ context.Context, _ ...*dao.Parameter) ([]*T, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*T, 0, len(s.records))
	for _, v := range s.records {
		out = append(out, v)
	}
	return out, nil
}
