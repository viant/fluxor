package workflow

import (
	"context"
	"github.com/viant/fluxor/model/execution"
	"github.com/viant/fluxor/model/types"
	"github.com/viant/fluxor/service/dao"
	"github.com/viant/fluxor/service/dao/workflow"
	"github.com/viant/fluxor/service/processor"
	"reflect"
)

const name = "workflow"

// Service extracts structured information from LLM responses
type Service struct {
	processor   *processor.Service
	workflowDao *workflow.Service
	processDao  dao.Service[string, execution.Process]
}

// New creates a new extractor service
func New(processor *processor.Service, workflowDao *workflow.Service, processDao dao.Service[string, execution.Process]) *Service {
	return &Service{
		processor:   processor,
		workflowDao: workflowDao,
	}
}

// Name returns the service name
func (s *Service) Name() string {
	return name
}

// Methods returns the service methods
func (s *Service) Methods() types.Signatures {
	return []types.Signature{
		{
			Name:   "status",
			Input:  reflect.TypeOf(&StatusInput{}),
			Output: reflect.TypeOf(&StartOutput{}),
		},
		{
			Name:   "status",
			Input:  reflect.TypeOf(&StartInput{}),
			Output: reflect.TypeOf(&StartOutput{}),
		},
		{
			Name:   "wait",
			Input:  reflect.TypeOf(&WaitInput{}),
			Output: reflect.TypeOf(&WaitOutput{}),
		},
	}
}

// Method returns the specified method
func (s *Service) Method(name string) (types.Executable, error) {
	switch name {
	case "start":
		return s.start, nil
	case "status":
		return s.status, nil
	case "wait":
		return s.wait, nil

	default:
		return nil, types.NewMethodNotFoundError(name)
	}
}

func (s *Service) ensureWorkflow(ctx context.Context, input *StartInput) error {
	if input.Workflow != nil {
		return nil
	}
	workflow, err := s.workflowDao.Load(ctx, input.Location)
	if err != nil {
		return err
	}
	input.Workflow = workflow
	return nil
}
