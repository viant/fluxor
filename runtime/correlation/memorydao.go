package correlation

import (
	"context"
	"sync"
)

// MemoryDAO stores correlation groups purely in memory; useful for unit tests
// and single-instance deployments.
type MemoryDAO struct {
	mu     sync.RWMutex
	groups map[string]*Group
}

func NewMemoryDAO() *MemoryDAO {
	return &MemoryDAO{groups: make(map[string]*Group)}
}

func (d *MemoryDAO) Save(_ context.Context, g *Group) error {
	if g == nil {
		return nil
	}
	d.mu.Lock()
	d.groups[g.ID] = g
	d.mu.Unlock()
	return nil
}

func (d *MemoryDAO) Load(_ context.Context, id string) (*Group, error) {
	d.mu.RLock()
	g := d.groups[id]
	d.mu.RUnlock()
	return g, nil
}

func (d *MemoryDAO) Delete(_ context.Context, id string) error {
	d.mu.Lock()
	delete(d.groups, id)
	d.mu.Unlock()
	return nil
}

func (d *MemoryDAO) List(_ context.Context) ([]*Group, error) {
	d.mu.RLock()
	out := make([]*Group, 0, len(d.groups))
	for _, g := range d.groups {
		out = append(out, g)
	}
	d.mu.RUnlock()
	return out, nil
}
