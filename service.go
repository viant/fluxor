package fluxor

import (
	"github.com/viant/afs"
	"github.com/viant/afs/storage"
	"github.com/viant/fluxor/extension"
	"github.com/viant/fluxor/model/execution"
	"github.com/viant/fluxor/model/types"
	"github.com/viant/fluxor/service/action/nop"
	"github.com/viant/fluxor/service/action/printer"
	aexecutor "github.com/viant/fluxor/service/action/system/executor"
	asecret "github.com/viant/fluxor/service/action/system/secret"
	astorage "github.com/viant/fluxor/service/action/system/storage"
	aworkflow "github.com/viant/fluxor/service/action/workflow"
	"github.com/viant/fluxor/service/allocator"
	ememory "github.com/viant/fluxor/service/dao/execution/memory"
	pmemory "github.com/viant/fluxor/service/dao/process/memory"
	"github.com/viant/fluxor/service/dao/workflow"
	"github.com/viant/fluxor/service/executor"
	texecutor "github.com/viant/fluxor/service/executor"
	"github.com/viant/fluxor/service/messaging"
	mmemory "github.com/viant/fluxor/service/messaging/memory"
	"github.com/viant/fluxor/service/meta"
	"github.com/viant/fluxor/service/processor"

	"github.com/viant/x"
)

type Service struct {
	runtime           *Runtime
	metaService       *meta.Service
	actions           *extension.Actions
	extensionTypes    []*x.Type
	extensionServices []types.Service
	executor          executor.Service
	queue             messaging.Queue[execution.Execution]
	rootTaskNodeName  string
	metaBaseURL       string
	metaFsOptions     []storage.Option
	processorWorkers  int
}

func (s *Service) init(options []Option) {
	for _, option := range options {
		option(s)
	}
	s.ensureBaseSetup()
	s.actions = extension.NewActions(s.extensionTypes...)
	s.executor = texecutor.NewService(s.actions)
	s.runtime.processor, _ = processor.New(
		processor.WithTaskExecutor(s.executor),
		processor.WithMessageQueue(s.queue),
		processor.WithWorkers(1),
		processor.WithTaskExecutionDAO(s.runtime.taskExecutionDao),
		processor.WithProcessDAO(s.runtime.processorDAO))
	s.actions.Register(printer.New())
	s.actions.Register(aexecutor.New())
	s.actions.Register(astorage.New())
	s.actions.Register(asecret.New())
	s.actions.Register(nop.New())
	for _, service := range s.extensionServices {
		s.actions.Register(service)
	}
	s.runtime.workflowService = aworkflow.New(s.runtime.processor, s.runtime.workflowDAO, s.runtime.processorDAO)
	s.actions.Register(s.runtime.workflowService)
	s.runtime.allocator = allocator.New(s.runtime.processorDAO, s.runtime.taskExecutionDao, s.queue, allocator.DefaultConfig())

}

func (s *Service) RegisterExtensionTypes(types ...*x.Type) {
	for i := range types {
		s.actions.Types().Register(types[i])
	}
}

func (s *Service) RegisterExtensionServices(services ...types.Service) {
	for i := range services {
		s.actions.Register(services[i])
	}
}

func (s *Service) Runtime() *Runtime {
	return s.runtime
}

func (s *Service) ensureBaseSetup() {

	if s.metaService == nil {
		s.metaService = meta.New(afs.New(), s.metaBaseURL, s.metaFsOptions...)
	}

	if s.runtime.workflowDAO == nil {
		if s.rootTaskNodeName == "" {
			s.rootTaskNodeName = "pipeline"
		}
		s.runtime.workflowDAO = workflow.New(workflow.WithRootTaskNodeName(s.rootTaskNodeName), workflow.WithMetaService(s.metaService))
	}
	if s.queue == nil {
		s.queue = mmemory.NewQueue[execution.Execution](mmemory.DefaultConfig())
	}
	if s.runtime.processorDAO == nil {
		s.runtime.processorDAO = pmemory.New()
	}
	if s.runtime.taskExecutionDao == nil {
		s.runtime.taskExecutionDao = ememory.New()
	}
}

func (s *Service) RegisterExtensionType(aType *x.Type) {
	s.extensionTypes = append(s.extensionTypes, aType)
}

func New(options ...Option) *Service {
	ret := &Service{runtime: &Runtime{}}
	ret.init(options)
	return ret
}
