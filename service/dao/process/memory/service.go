package memory

import (
	"context"
	"errors"
	"github.com/viant/fluxor/model/execution"
	"github.com/viant/fluxor/service/dao"
	"github.com/viant/fluxor/service/dao/criteria"
	"sync"
)

// Service implements an in-memory process storage
type Service struct {
	processes map[string]*execution.Process
	mux       sync.RWMutex
}

// Ensure Service implements dao.Service
var _ dao.Service[string, execution.Process] = (*Service)(nil)

// Save stores a process in memory
func (s *Service) Save(ctx context.Context, process *execution.Process) error {
	if process == nil {
		return errors.New("cannot save nil process")
	}
	if process.ID == "" {
		return errors.New("process ID cannot be empty")
	}

	s.mux.Lock()
	defer s.mux.Unlock()
	s.processes[process.ID] = process
	return nil
}

// Load retrieves a process by ID from memory
func (s *Service) Load(ctx context.Context, id string) (*execution.Process, error) {
	if id == "" {
		return nil, errors.New("process ID cannot be empty")
	}

	s.mux.RLock()
	defer s.mux.RUnlock()

	process, exists := s.processes[id]
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

	if _, exists := s.processes[id]; !exists {
		return errors.New("process not found")
	}
	delete(s.processes, id)
	return nil
}

// List returns all processes from memory
func (s *Service) List(ctx context.Context, parameters ...*dao.Parameter) ([]*execution.Process, error) {
	s.mux.RLock()
	defer s.mux.RUnlock()

	processList := make([]*execution.Process, 0, len(s.processes))
	for _, process := range s.processes {
		if !criteria.FilterByState(process.State, parameters) {
			continue
		}
		processList = append(processList, process)
	}
	return processList, nil
}

// New creates a new in-memory process storage service
func New() *Service {
	return &Service{
		processes: make(map[string]*execution.Process),
	}
}
