package fluxor

import (
	"github.com/viant/afs/storage"
	"github.com/viant/fluxor/model/execution"
	"github.com/viant/fluxor/model/types"
	"github.com/viant/fluxor/service/dao"
	"github.com/viant/fluxor/service/messaging"
	"github.com/viant/fluxor/service/meta"
	"github.com/viant/x"
)

// Service represents fluxor service
type Option func(s *Service)

// WithExtensionTypes sets the extension types
func WithExtensionTypes(types ...*x.Type) Option {
	return func(s *Service) {
		s.extensionTypes = types
	}
}

func WithMetaService(service *meta.Service) Option {
	return func(s *Service) {
		s.metaService = service
	}
}

// WithExtensionServices sets the extension services
func WithExtensionServices(services ...types.Service) Option {
	return func(s *Service) {
		s.extensionServices = services
	}
}

// WithQueue sets the message queue
func WithQueue(queue messaging.Queue[execution.Execution]) Option {
	return func(s *Service) {
		s.queue = queue
	}
}

// WithRootTaskNodeName sets the root task node name
func WithRootTaskNodeName(name string) Option {
	return func(s *Service) {
		s.rootTaskNodeName = name
	}
}

// WithProcessorDAO sets the processor DAO
func WithProcessDAO(dao dao.Service[string, execution.Process]) Option {
	return func(s *Service) {
		s.runtime.processorDAO = dao
	}
}

// WithTaskExecutionDAO sets the task execution DAO
func WithTaskExecutionDAO(dao dao.Service[string, execution.Execution]) Option {
	return func(s *Service) {
		s.runtime.taskExecutionDao = dao
	}
}

// WithProcessorWorkers sets the processor workers
func WithProcessorWorkers(count int) Option {
	return func(s *Service) {
		s.processorWorkers = count
	}
}

// WithMetaBaseURL sets the meta base URL
func WithMetaBaseURL(url string) Option {
	return func(s *Service) {
		s.metaBaseURL = url
	}
}

// WithMetaFsOptions with meta file system options
func WithMetaFsOptions(options ...storage.Option) Option {
	return func(s *Service) {
		s.metaFsOptions = options
	}
}
