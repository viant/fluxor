package workflow

import (
	"context"
	"fmt"
	"github.com/viant/fluxor/model"
	"github.com/viant/fluxor/model/types"
	"github.com/viant/fluxor/runtime/execution"
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
		processDao:  processDao,
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
			Name:        "status",
			Description: "Retrieves the current state and output of a workflow process based on its process ID.",
			Input:       reflect.TypeOf(&StatusInput{}),
			Output:      reflect.TypeOf(&RunOutput{}),
		},
		{
			Name:        "run",
			Description: "Executes a workflow with the given definition and parameters, returning the process ID, output, errors, state, and optional trace information.",
			Input:       reflect.TypeOf(&RunInput{}),
			Output:      reflect.TypeOf(&RunOutput{}),
		},
		{
			Name:        "wait",
			Description: "Polls a workflow process until completion or timeout, returning its final state, output, errors, and timing information.",
			Input:       reflect.TypeOf(&WaitInput{}),
			Output:      reflect.TypeOf(&WaitOutput{}),
		},
	}
}

// Method returns the specified method
func (s *Service) Method(name string) (types.Executable, error) {
	switch name {
	case "run":
		return s.run, nil
	case "status":
		return s.status, nil
	case "wait":
		return s.wait, nil

	default:
		return nil, types.NewMethodNotFoundError(name)
	}
}

func (s *Service) ensureWorkflow(ctx context.Context, input *RunInput) error {
	if input.Workflow != nil {
		return nil
	}
	var aWorkflow *model.Workflow
	var err error
	if len(input.Source) > 0 {
		aWorkflow, err = s.workflowDao.DecodeYAML(input.Source)
	} else {
		aWorkflow, err = s.workflowDao.Load(ctx, input.Location)
	}
	if err != nil {
		return err
	}
	if aWorkflow.Pipeline == nil {
		return fmt.Errorf("workflow %v has no %v", input.Location, s.workflowDao.RootTaskNodeName())
	}
	input.Workflow = aWorkflow
	return nil
}
