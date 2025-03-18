package memory

import (
	"context"
	"errors"
	"github.com/viant/fluxor/model/execution"
	"github.com/viant/fluxor/service/dao"
	"sync"
)

// Service implements an in-memory process storage
type Service struct {
	executions map[string]*execution.Execution
	mux        sync.RWMutex
}

// Ensure Service implements dao.Service
var _ dao.Service[string, execution.Execution] = (*Service)(nil)

// Save stores a process in memory
func (s *Service) Save(ctx context.Context, execution *execution.Execution) error {
	if execution == nil {
		return errors.New("cannot save nil execution")
	}
	if execution.ID == "" {
		return errors.New("execution ID cannot be empty")
	}

	s.mux.Lock()
	defer s.mux.Unlock()
	s.executions[execution.ID] = execution
	return nil
}

// Load retrieves a process by ID from memory
func (s *Service) Load(ctx context.Context, id string) (*execution.Execution, error) {
	if id == "" {
		return nil, errors.New("process ID cannot be empty")
	}

	s.mux.RLock()
	defer s.mux.RUnlock()

	process, exists := s.executions[id]
	if !exists {
		return nil, errors.New("process not found")
	}
	return process, nil
}

// Delete removes a process from memory
func (s *Service) Delete(ctx context.Context, id string) error {
	if id == "" {
		return errors.New("process ID cannot be empty")
	}

	s.mux.Lock()
	defer s.mux.Unlock()

	if _, exists := s.executions[id]; !exists {
		return errors.New("process not found")
	}
	delete(s.executions, id)
	return nil
}

// List returns all executions from memory
func (s *Service) List(ctx context.Context, parameters ...*dao.Parameter) ([]*execution.Execution, error) {
	s.mux.RLock()
	defer s.mux.RUnlock()

	list := make([]*execution.Execution, 0, len(s.executions))
	for _, process := range s.executions {
		list = append(list, process)
	}
	return list, nil
}

// New creates a new in-memory process storage service
func New() *Service {
	return &Service{
		executions: make(map[string]*execution.Execution),
	}
}
