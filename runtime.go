package fluxor

import (
	"context"
	"fmt"
	"time"

	"github.com/viant/fluxor/model"
	execution "github.com/viant/fluxor/runtime/execution"
	aworkflow "github.com/viant/fluxor/service/action/workflow"
	"github.com/viant/fluxor/service/allocator"
	"github.com/viant/fluxor/service/dao"
	"github.com/viant/fluxor/service/dao/workflow"
	"github.com/viant/fluxor/service/messaging"
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
	// queue is the shared execution queue (processor inbound)
	queue messaging.Queue[execution.Execution]
}

// LoadWorkflow loads a workflow
func (r *Runtime) LoadWorkflow(ctx context.Context, location string) (*model.Workflow, error) {
	return r.workflowDAO.Load(ctx, location)
}

// DecideYAMLWorkflow loads a workflow
func (r *Runtime) DecodeYAMLWorkflow(data []byte) (*model.Workflow, error) {
	return r.workflowDAO.DecodeYAML(data)
}

// StartProcess starts a new process
func (r *Runtime) StartProcess(ctx context.Context, aWorkflow *model.Workflow, initialState map[string]interface{}, tasks ...string) (*execution.Process, execution.Wait, error) {
	process, err := r.processor.StartProcess(ctx, aWorkflow, initialState, tasks...)
	if err != nil {
		return nil, nil, err
	}
	wait := func(ctx context.Context, timeout time.Duration) (*execution.ProcessOutput, error) {
		output, err := r.workflowService.WaitForProcess(ctx, process.ID, int(timeout.Milliseconds()))
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

// QueueExecution publishes an ad-hoc execution to the processor queue.
func (r *Runtime) QueueExecution(ctx context.Context, exec *execution.Execution) error {
	return r.queue.Publish(ctx, exec)
}

// WaitForExecution waits until the execution reaches a terminal state or the timeout expires.
// WaitForExecution polls the task-execution DAO until the execution enters a terminal
// or paused/approval state, or the timeout expires.  It returns the execution as soon
// as its State is one of Completed, Failed, Skipped, Cancelled, Paused, or WaitForApproval.
func (r *Runtime) WaitForExecution(
	ctx context.Context,
	execID string,
	timeout time.Duration,
) (*execution.Execution, error) {
	deadline := time.Now().Add(timeout)
	for {
		exec, err := r.taskExecutionDao.Load(ctx, execID)
		if err != nil {
			return nil, err
		}
		switch exec.State {
		case execution.TaskStateCompleted,
			execution.TaskStateFailed,
			execution.TaskStateSkipped,
			execution.TaskStateCancelled,
			execution.TaskStatePaused:
			return exec, nil
		default:
			if exec.State.IsWaitForApproval() {
				return exec, nil
			}
		}
		if time.Now().After(deadline) {
			return exec, fmt.Errorf("timeout waiting for execution %q", execID)
		}
		time.Sleep(100 * time.Millisecond)
	}
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
