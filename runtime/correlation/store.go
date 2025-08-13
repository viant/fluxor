package correlation

import "sync"

// Store is an in-memory implementation satisfying basic operations needed by
// the allocator/processor communication.  It can be replaced by a Redis or DB
// implementation later without changing callers.
type Store struct {
	mu     sync.RWMutex
	groups map[string]*Group
}

// Iterate executes fn for each group under read lock.
func (s *Store) Iterate(fn func(id string, g *Group)) {
	s.mu.RLock()
	for id, g := range s.groups {
		fn(id, g)
	}
	s.mu.RUnlock()
}

func NewStore() *Store {
	return &Store{groups: make(map[string]*Group)}
}

// Create registers a new group. If it already exists the existing pointer is
// returned.
func (s *Store) Create(g *Group) *Group {
	s.mu.Lock()
	if existing, ok := s.groups[g.ID]; ok {
		s.mu.Unlock()
		return existing
	}
	s.groups[g.ID] = g
	s.mu.Unlock()
	return g
}

func (s *Store) Get(id string) *Group {
	s.mu.RLock()
	g := s.groups[id]
	s.mu.RUnlock()
	return g
}

func (s *Store) Delete(id string) {
	s.mu.Lock()
	delete(s.groups, id)
	s.mu.Unlock()
}
