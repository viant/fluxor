package fluxor

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"time"

	"github.com/viant/fluxor/internal/clock"

	"github.com/viant/fluxor/model"
	"github.com/viant/fluxor/runtime/execution"
	aworkflow "github.com/viant/fluxor/service/action/workflow"
	"github.com/viant/fluxor/service/allocator"
	"github.com/viant/fluxor/service/dao"
	"github.com/viant/fluxor/service/dao/workflow"
	"github.com/viant/fluxor/service/messaging"
	"github.com/viant/fluxor/service/processor"
)

// ---------------------------------------------------------------------------
// Workflow hot-swap helpers
// ---------------------------------------------------------------------------

// RefreshWorkflow discards any cached copy of the workflow definition located
// at the given URL/location. The next LoadWorkflow call will reload the file
// via the configured meta-service (i.e. one extra disk/cloud round-trip).
func (r *Runtime) RefreshWorkflow(location string) error {
	if r == nil || r.workflowDAO == nil {
		return fmt.Errorf("runtime not fully initialised – workflowDAO missing")
	}
	r.workflowDAO.Refresh(location)
	return nil
}

// UpsertDefinition parses the supplied YAML bytes and stores the resulting
// workflow definition in the in-memory cache under the specified location.
// When data is nil the call falls back to RefreshWorkflow, causing a lazy
// reload on next use.
func (r *Runtime) UpsertDefinition(location string, data []byte) error {
	if r == nil || r.workflowDAO == nil {
		return fmt.Errorf("runtime not fully initialised – workflowDAO missing")
	}

	// If no data provided, fall back to lazy refresh (strategy #1).
	if data == nil {
		return r.RefreshWorkflow(location)
	}

	// Parse the YAML using the DAO's decoding logic.
	wf, err := r.workflowDAO.DecodeYAML(data)
	if err != nil {
		return fmt.Errorf("failed to decode workflow YAML: %w", err)
	}

	// Ensure the Source URL mirrors the provided location so that any
	// downstream code relying on it sees the expected value.
	if wf.Source == nil {
		wf.Source = &model.Source{URL: location}
	} else {
		wf.Source.URL = location
	}

	// Store in cache for immediate availability.
	r.workflowDAO.Upsert(location, wf)
	return nil
}

// ---------------------------------------------------------------------------
// Convenience helpers
// ---------------------------------------------------------------------------

// RunTaskOnce is a convenience helper that executes a *single* task from the
// supplied workflow and waits for its completion.  It is intended for quick
// ad-hoc jobs, debugging and unit tests where launching the entire workflow
// would be unnecessary overhead.
//
// The helper works by submitting an "at-hoc" execution to the shared
// allocator/processor queue, therefore semantics (retries, policies, tracing
// etc.) are identical to regular executions.  The returned value is whatever
// the task's action populates as its output.
func (r *Runtime) RunTaskOnce(ctx context.Context, wf *model.Workflow, taskID string, input interface{}) (interface{}, error) {
	if wf == nil {
		return nil, fmt.Errorf("workflow is nil")
	}
	// Locate the requested task definition.
	task := wf.AllTasks()[taskID]
	if task == nil {
		return nil, fmt.Errorf("task %q not found in workflow %q", taskID, wf.Name)
	}
	if task.Action == nil {
		return nil, fmt.Errorf("task %q has no action defined", taskID)
	}
	// Build an at-hoc execution describing the task.
	exec := &execution.Execution{
		ID:          uuid.New().String(),
		AtHoc:       true,
		Service:     task.Action.Service,
		Method:      task.Action.Method,
		Input:       input,
		State:       execution.TaskStatePending,
		ScheduledAt: clock.Now(),
	}
	// Submit to the runtime.
	waitFn, err := r.ScheduleExecution(ctx, exec)
	if err != nil {
		return nil, err
	}
	// Block until completion (use a generous default timeout).
	const defaultTimeout = 5 * time.Minute
	exec, err = waitFn(defaultTimeout)
	if err != nil {
		return nil, err
	}
	return exec.Output, nil
}

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
		case execution.TaskStateWaitForApproval:
			// If the task has been explicitly rejected we can finish right away – the
			// execution will never proceed.
			if exec.Approved != nil && !*exec.Approved {
				return exec, nil
			}
		case execution.TaskStatePending:
			// After a positive decision the approval service rewinds the State back to
			// pending and republishes the message so that the processor can resume
			// work.  We only finish early on explicit rejection to avoid an
			// indefinite wait.
			if exec.Approved != nil && !*exec.Approved {
				return exec, nil
			}
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
