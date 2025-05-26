package memory

import (
	"context"
	"github.com/viant/fluxor/model/execution"
	"github.com/viant/fluxor/service/dao"
	"sync"
)

// Service implements an in-memory execution storage.  All operations are
// thread-safe and return **copies** of the underlying objects to prevent data
// races when callers mutate the returned instances.
type Service struct {
	executions map[string]*execution.Execution
	mux        sync.RWMutex
}

// Compile-time check that Service implements the generic DAO interface.
var _ dao.Service[string, execution.Execution] = (*Service)(nil)

// Save persists (a clone of) the supplied execution.
func (s *Service) Save(_ context.Context, e *execution.Execution) error {
	if e == nil {
		return dao.ErrNilEntity
	}
	if e.ID == "" {
		return dao.ErrInvalidID
	}

	s.mux.Lock()
	defer s.mux.Unlock()

	s.executions[e.ID] = e.Clone()
	return nil
}

// Load retrieves a copy of the execution or dao.ErrNotFound.
func (s *Service) Load(_ context.Context, id string) (*execution.Execution, error) {
	if id == "" {
		return nil, dao.ErrInvalidID
	}

	s.mux.RLock()
	e, ok := s.executions[id]
	s.mux.RUnlock()

	if !ok {
		return nil, dao.ErrNotFound
	}
	return e.Clone(), nil
}

// Delete removes an execution.
func (s *Service) Delete(_ context.Context, id string) error {
	if id == "" {
		return dao.ErrInvalidID
	}

	s.mux.Lock()
	defer s.mux.Unlock()

	if _, ok := s.executions[id]; !ok {
		return dao.ErrNotFound
	}
	delete(s.executions, id)
	return nil
}

// List returns shallow copies of all executions.  Parameter filtering is not
// implemented for the in-memory store.
func (s *Service) List(_ context.Context, _ ...*dao.Parameter) ([]*execution.Execution, error) {
	s.mux.RLock()
	defer s.mux.RUnlock()

	out := make([]*execution.Execution, 0, len(s.executions))
	for _, e := range s.executions {
		out = append(out, e.Clone())
	}
	return out, nil
}

// New constructor.
func New() *Service {
	return &Service{executions: map[string]*execution.Execution{}}
}

// (no package-private helpers – model/execution provides Clone())
