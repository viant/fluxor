package fluxor

import (
	"context"
	"github.com/viant/fluxor/model"
	"github.com/viant/fluxor/model/execution"
	aworkflow "github.com/viant/fluxor/service/action/workflow"
	"github.com/viant/fluxor/service/allocator"
	"github.com/viant/fluxor/service/dao"
	"github.com/viant/fluxor/service/dao/workflow"
	"github.com/viant/fluxor/service/processor"
)

// Runtime represents a workflow engine runtime
type Runtime struct {
	workflowService  *aworkflow.Service
	workflowDAO      *workflow.Service
	processorDAO     dao.Service[string, execution.Process]
	taskExecutionDao dao.Service[string, execution.Execution]
	processor        *processor.Service
	allocator        *allocator.Service
}

// LoadWorkflow loads a workflow
func (r *Runtime) LoadWorkflow(ctx context.Context, location string) (*model.Workflow, error) {
	return r.workflowDAO.Load(ctx, location)
}

// StartProcess starts a new process
func (r *Runtime) StartProcess(ctx context.Context, aWorkflow *model.Workflow, initialState map[string]interface{}, tasks ...string) (*execution.Process, func(ctx context.Context, timeoutMs int) (*execution.ProcessOutput, error), error) {
	process, err := r.processor.StartProcess(ctx, aWorkflow, tasks, initialState)
	if err != nil {
		return nil, nil, err
	}
	wait := func(ctx context.Context, timeoutMs int) (*execution.ProcessOutput, error) {
		output, err := r.workflowService.WaitForProcess(ctx, process.ID, timeoutMs)
		if err != nil {
			return nil, err
		}
		return (*execution.ProcessOutput)(output), nil
	}
	return process, wait, nil
}

// Start starts runtime
func (r *Runtime) Start(ctx context.Context) error {
	go r.processor.Start(ctx)
	go r.allocator.Start(ctx)
	return nil
}

// Shutdown shutdowns runtime
func (r *Runtime) Shutdown(ctx context.Context) error {
	r.processor.Shutdown()
	r.allocator.Shutdown()
	return nil
}

// Process returns a process
func (r *Runtime) Process(ctx context.Context, id string) (*execution.Process, error) {
	return r.processorDAO.Load(ctx, id)
}

// Execution returns an execution
func (r *Runtime) Execution(ctx context.Context, id string) (*execution.Execution, error) {
	return r.taskExecutionDao.Load(ctx, id)
}

// Processes saves execution
func (r *Runtime) SaveExecution(ctx context.Context, anExecution *execution.Execution) error {
	return r.taskExecutionDao.Save(ctx, anExecution)
}

// Processes returns a list of processes
func (r *Runtime) Processes(ctx context.Context, parameter ...*dao.Parameter) ([]*execution.Process, error) {
	return r.processorDAO.List(ctx, parameter...)
}
