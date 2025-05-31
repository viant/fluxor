package memory

import (
	"context"
	"github.com/viant/fluxor/runtime/execution"
	"github.com/viant/fluxor/service/dao"
	"github.com/viant/fluxor/service/dao/criteria"
	"sync"
)

// Service implements an in-memory, thread-safe store for processes.  All API
// methods work with copies to eliminate data races between goroutines.
type Service struct {
	processes map[string]*execution.Process
	mux       sync.RWMutex
}

var _ dao.Service[string, execution.Process] = (*Service)(nil)

func (s *Service) Save(_ context.Context, p *execution.Process) error {
	if p == nil {
		return dao.ErrNilEntity
	}
	if p.ID == "" {
		return dao.ErrInvalidID
	}

	s.mux.Lock()
	defer s.mux.Unlock()

	if existing, ok := s.processes[p.ID]; ok && existing != nil {
		existing.CopyFrom(p)
	} else {
		s.processes[p.ID] = p
	}
	return nil
}

func (s *Service) Load(_ context.Context, id string) (*execution.Process, error) {
	if id == "" {
		return nil, dao.ErrInvalidID
	}

	s.mux.RLock()
	p, ok := s.processes[id]
	s.mux.RUnlock()

	if !ok {
		return nil, dao.ErrNotFound
	}
	return p, nil
}

func (s *Service) Delete(_ context.Context, id string) error {
	if id == "" {
		return dao.ErrInvalidID
	}

	s.mux.Lock()
	defer s.mux.Unlock()

	if _, ok := s.processes[id]; !ok {
		return dao.ErrNotFound
	}
	delete(s.processes, id)
	return nil
}

func (s *Service) List(_ context.Context, parameters ...*dao.Parameter) ([]*execution.Process, error) {
	s.mux.RLock()
	defer s.mux.RUnlock()

	out := make([]*execution.Process, 0, len(s.processes))
	for _, p := range s.processes {
		if !criteria.FilterByState(p.State, parameters) {
			continue
		}
		out = append(out, p)
	}
	return out, nil
}

func New() *Service {
	return &Service{processes: map[string]*execution.Process{}}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------
