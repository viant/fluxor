package processor

import (
	execution "github.com/viant/fluxor/runtime/execution"
	"github.com/viant/fluxor/service/dao"
	"github.com/viant/fluxor/service/executor"
	"github.com/viant/fluxor/service/messaging"
)

// Package executor provides a service executor.
type Option func(*Service)

// WithProcessDAO sets the process store implementation
func WithProcessDAO(processDAO dao.Service[string, execution.Process]) Option {
	return func(s *Service) {
		s.processDAO = processDAO
	}
}

func WithTaskExecutionDAO(taskExecutionDao dao.Service[string, execution.Execution]) Option {
	return func(s *Service) {
		s.taskExecutionDao = taskExecutionDao
	}
}

// WithMessageQueue sets the message queue implementation
func WithMessageQueue(queue messaging.Queue[execution.Execution]) Option {
	return func(s *Service) {
		s.queue = queue
	}
}

// WithTaskExecutor sets a custom task executor function
func WithTaskExecutor(executor executor.Service) Option {
	return func(s *Service) {
		s.executor = executor
	}
}

// WithWorkers sets the number of worker goroutines
func WithWorkers(count int) Option {
	return func(s *Service) {
		s.config.WorkerCount = count
	}
}

// WithExecutor sets the task executor for the service
func WithExecutor(executor executor.Service) Option {
	return func(s *Service) {
		s.executor = executor
	}
}

// WithSessionListeners registers immutable state listeners that will be copied
// to every Session created during task execution.
func WithSessionListeners(fns ...execution.StateListener) Option {
	return func(s *Service) {
		if len(fns) == 0 {
			return
		}
		s.sessListeners = append(s.sessListeners, fns...)
	}
}

// WithWhenListeners registers callbacks invoked after every when-condition
// evaluation.
func WithWhenListeners(fns ...execution.WhenListener) Option {
	return func(s *Service) {
		if len(fns) == 0 {
			return
		}
		s.whenListeners = append(s.whenListeners, fns...)
	}
}

// WithConfig sets the configuration for the service
func WithConfig(config Config) Option {
	return func(s *Service) {
		s.config = config
	}
}
