package fluxor

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"time"

	"github.com/viant/fluxor/model"
	"github.com/viant/fluxor/runtime/execution"
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

// DecodeYAMLWorkflow loads a workflow
func (r *Runtime) DecodeYAMLWorkflow(data []byte) (*model.Workflow, error) {
	return r.workflowDAO.DecodeYAML(data)
}

// ProcessFromContext return process from context
func (r *Runtime) ProcessFromContext(ctx context.Context) *execution.Process {
	// If the incoming context contains a running parent process, record its ID
	if parentProc := execution.ContextValue[*execution.Process](ctx); parentProc != nil {
		return parentProc
	}
	return nil
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

func (r *Runtime) ScheduleExecution(ctx context.Context, exec *execution.Execution) (func(duration time.Duration) (*execution.Execution, error), error) {
	var err error
	if exec.ID == "" {
		exec.ID = uuid.New().String()
	}
	aProcess := r.ProcessFromContext(ctx)
	if aProcess == nil {
		aProcess, err = r.processor.NewProcess(ctx, exec.ID, &model.Workflow{}, map[string]interface{}{})
		if err != nil {
			return nil, err
		}
	}

	// Handle ad-hoc executions (single task outside a workflow): register a
	// synthetic task so that later look-ups succeed.
	if exec.AtHoc {
		if task := exec.AtHocTask(); task != nil {
			aProcess.RegisterTask(task)
			exec.TaskID = task.ID
		}
	}

	exec.ProcessID = aProcess.ID
	r.taskExecutionDao.Save(ctx, exec)
	if err = r.queue.Publish(ctx, exec); err != nil {
		return nil, err
	}
	return func(timeout time.Duration) (*execution.Execution, error) {
		exec, err = r.waitForExecution(ctx, exec.ID, timeout)
		if err != nil {
			return nil, err
		}
		return exec, nil
	}, nil
}

func (r *Runtime) waitForExecution(
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

// Processes returns a list of processes
func (r *Runtime) Processes(ctx context.Context, parameter ...*dao.Parameter) ([]*execution.Process, error) {
	return r.processorDAO.List(ctx, parameter...)
}
