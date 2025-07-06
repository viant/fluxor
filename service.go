package fluxor

import (
	"context"
	"github.com/viant/afs"
	"github.com/viant/afs/storage"
	"github.com/viant/fluxor/extension"
	"github.com/viant/fluxor/model/types"
	"github.com/viant/fluxor/runtime/execution"
	"github.com/viant/fluxor/service/action/nop"
	"github.com/viant/fluxor/service/action/printer"
	"github.com/viant/fluxor/service/action/system/exec"
	"github.com/viant/fluxor/service/action/system/patch"
	aworkflow "github.com/viant/fluxor/service/action/workflow"
	"github.com/viant/fluxor/service/allocator"
	"github.com/viant/fluxor/service/approval"
	"github.com/viant/fluxor/service/approval/memory"
	ememory "github.com/viant/fluxor/service/dao/execution/memory"
	pmemory "github.com/viant/fluxor/service/dao/process/memory"
	"github.com/viant/fluxor/service/dao/workflow"
	"github.com/viant/fluxor/service/event"
	"github.com/viant/fluxor/service/executor"
	texecutor "github.com/viant/fluxor/service/executor"
	"github.com/viant/fluxor/service/messaging"
	mmemory "github.com/viant/fluxor/service/messaging/memory"
	"github.com/viant/fluxor/service/meta"
	"github.com/viant/fluxor/service/processor"

	"github.com/viant/x"
)

type Service struct {
	config  *Config  `json:"config,omitempty"`
	runtime *Runtime `json:"runtime,omitempty"`

	approvalService   approval.Service
	metaService       *meta.Service                        `json:"metaService,omitempty"`
	extensionTypes    []*x.Type                            `json:"extensionTypes,omitempty"`
	extensionServices []types.Service                      `json:"extensionServices,omitempty"`
	executor          executor.Service                     `json:"executor,omitempty"`
	queue             messaging.Queue[execution.Execution] `json:"queue,omitempty"`
	rootTaskNodeName  string                               `json:"rootTaskNodeName,omitempty"`
	metaBaseURL       string                               `json:"metaBaseURL,omitempty"`
	metaFsOptions     []storage.Option                     `json:"metaFsOptions,omitempty"`
	processorWorkers  int                                  `json:"processorWorkers,omitempty"`
	actions           *extension.Actions                   `json:"actions,omitempty"`
	eventService      *event.Service                       `json:"eventService,omitempty"`
	executorOptions   []texecutor.Option                   `json:"-"`
	stateListeners    []execution.StateListener            `json:"-"`
	whenListeners     []execution.WhenListener             `json:"-"`
}

func (s *Service) init(options []Option) {
	for _, option := range options {
		option(s)
	}
	s.ensureBaseSetup()
	s.actions = extension.NewActions(s.extensionTypes...)
	s.approvalService = memory.New(
		s.runtime.taskExecutionDao,
		memory.WithProcessDAO(s.runtime.processorDAO),
		memory.WithExecutionQueue(s.queue),
	)

	s.executorOptions = append(s.executorOptions, texecutor.WithApprovalService(s.approvalService))
	// Create executor after injecting approvalService option
	s.executor = texecutor.NewService(s.actions, s.executorOptions...)

	workers := s.config.Processor.WorkerCount
	if s.processorWorkers > 0 {
		workers = s.processorWorkers
	}
	s.runtime.processor, _ = processor.New(
		processor.WithTaskExecutor(s.executor),
		processor.WithMessageQueue(s.queue),
		processor.WithWorkers(workers),
		processor.WithSessionListeners(s.stateListeners...),
		processor.WithWhenListeners(s.whenListeners...),
		processor.WithTaskExecutionDAO(s.runtime.taskExecutionDao),
		processor.WithProcessDAO(s.runtime.processorDAO))

	s.actions.Register(printer.New())
	s.actions.Register(exec.New())
	s.actions.Register(patch.New())
	s.actions.Register(nop.New())
	for _, service := range s.extensionServices {
		s.actions.Register(service)
	}
	s.runtime.workflowService = aworkflow.New(s.runtime.processor, s.runtime.workflowDAO, s.runtime.processorDAO)
	s.actions.Register(s.runtime.workflowService)
	s.runtime.allocator = allocator.New(s.runtime.processorDAO, s.runtime.taskExecutionDao, s.queue, allocator.DefaultConfig())
	// expose the shared queue on the runtime for ad-hoc executions
	s.runtime.queue = s.queue

	if s.eventService == nil {
		s.eventService, _ = event.New("memory", event.WithNewMemoryQueueConfig(mmemory.NamedConfig))
	}

}

// EventService returns event service
func (s *Service) EventService() *event.Service {
	return s.eventService
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

func (s *Service) ApprovalService() approval.Service {
	return s.approvalService
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

func (s *Service) NewContext(ctx context.Context) context.Context {
	return execution.NewContext(ctx, s.actions, s.eventService)
}

func (s *Service) Actions() *extension.Actions {
	return s.actions
}

func (s *Service) RegisterExtensionType(aType *x.Type) {
	s.extensionTypes = append(s.extensionTypes, aType)
}

func New(options ...Option) *Service {
	return NewFromConfig(nil, options...)
}

// NewFromConfig constructs the Fluxor engine using a declarative configuration
// that can be further customised by functional options. The precedence order
// is:
//  1. package defaults (via DefaultConfig)
//  2. values present in cfg (may be nil – treated as empty)
//  3. values set by Option functions (highest priority)
func NewFromConfig(cfg *Config, opts ...Option) *Service {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	if err := cfg.Validate(); err != nil {
		panic(err) // configuration error at startup is unrecoverable
	}

	ret := &Service{
		runtime: &Runtime{},
		config:  cfg,
	}

	// Translate selected cfg fields into pre-applied options so that option
	// functions provided by the caller can still override them.
	pre := []Option{}
	if cfg.Processor.WorkerCount > 0 {
		pre = append(pre, WithProcessorWorkers(cfg.Processor.WorkerCount))
	}

	// Apply config-derived options first, then caller-supplied ones.
	ret.init(append(pre, opts...))
	return ret
}
