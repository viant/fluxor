package orchestrator

import (
	"context"
	"fmt"
	"github.com/viant/fluxor/model/graph"
	"github.com/viant/fluxor/runtime/execution"
	"time"
)

// OrchestratorContextKey is the string key used to inject the orchestrator
// into the per-execution context. Processor copies session execution-context
// entries into the action context using these string keys.
const OrchestratorContextKey = "fluxor.orchestrator"

// Orchestrator exposes programmatic fan-out/fan-in helpers that tasks can use
// from inside their action implementation.
type Orchestrator struct {
	rt Runtime
}

// Runtime abstracts the minimal subset of fluxor.Runtime used by the
// orchestrator.  It is intentionally declared locally to avoid an import
// cycle between the root module and this package.
type Runtime interface {
	EmitExecutions(ctx context.Context, parent *execution.Execution, children []*execution.Execution) (string, error)
	WaitForUnblock(ctx context.Context, execID string, timeout time.Duration) (*execution.Execution, error)
	AwaitGroup(ctx context.Context, id string, timeout time.Duration) ([]interface{}, error)
	ScheduleExecution(ctx context.Context, exec *execution.Execution) (func(duration time.Duration) (*execution.Execution, error), error)
}

// NewOrchestrator wraps the provided runtime to expose programmatic emit/await
// APIs to task code.
func New(rt Runtime) *Orchestrator {
	if rt == nil {
		return nil
	}
	return &Orchestrator{rt: rt}
}

// FromContext extracts an orchestrator previously injected into the execution
// context. Callers should check the boolean return value.
func FromContext(ctx context.Context) (*Orchestrator, bool) {
	if ctx == nil {
		return nil, false
	}
	if v := ctx.Value(OrchestratorContextKey); v != nil {
		if o, ok := v.(*Orchestrator); ok {
			return o, true
		}
	}
	return nil, false
}

// EmitForEach clones the provided task template for each element in items,
// registers tasks in the current process and emits child executions. The
// current action's execution is transitioned into waitAsync.
func (o *Orchestrator) EmitForEach(ctx context.Context, items []interface{}, as string, template *graph.Task) (string, error) {
	if o == nil || o.rt == nil {
		return "", fmt.Errorf("orchestrator not initialised")
	}
	if template == nil {
		return "", fmt.Errorf("task template is nil")
	}
	parent := execution.ContextValue[*execution.Execution](ctx)
	process := execution.ContextValue[*execution.Process](ctx)
	if parent == nil || process == nil {
		return "", fmt.Errorf("missing execution/process in context")
	}
	// Locate the current task (parent) for id derivation.
	currentTask := process.LookupTask(parent.TaskID)
	if currentTask == nil {
		return "", fmt.Errorf("parent task %s not found", parent.TaskID)
	}

	// Build child executions
	children := make([]*execution.Execution, 0, len(items))
	for i, item := range items {
		clone := template.Clone()
		// Ensure stable id unique per item
		if clone.ID == "" {
			clone.ID = fmt.Sprintf("%s[%d]", currentTask.ID, i)
		} else {
			clone.ID = fmt.Sprintf("%s.%s[%d]", currentTask.ID, clone.ID, i)
		}
		// Register task so executor can look it up later
		process.RegisterTask(clone)
		child := execution.NewExecution(process.ID, currentTask, clone)
		if as != "" {
			child.Data = map[string]interface{}{as: item}
		}
		children = append(children, child)
	}
	return o.rt.EmitExecutions(ctx, parent, children)
}

// EmitChildren emits the supplied child executions and transitions the current
// action's execution into waitAsync.
func (o *Orchestrator) EmitChildren(ctx context.Context, children []*execution.Execution) (string, error) {
	if o == nil || o.rt == nil {
		return "", fmt.Errorf("orchestrator not initialised")
	}
	parent := execution.ContextValue[*execution.Execution](ctx)
	if parent == nil {
		return "", fmt.Errorf("missing execution in context")
	}
	return o.rt.EmitExecutions(ctx, parent, children)
}

// EmitOne is a convenience wrapper to emit a single child execution and create
// a correlation group with Expected=1.
func (o *Orchestrator) EmitOne(ctx context.Context, child *execution.Execution) (string, error) {
	if child == nil {
		return "", fmt.Errorf("child is nil")
	}
	return o.EmitChildren(ctx, []*execution.Execution{child})
}

// AwaitResume blocks until the current execution is unblocked by the
// allocator (i.e. the async correlation group completes). It returns the
// updated execution output (already merged by the allocator according to the
// await/merge strategy) or an error.
func (o *Orchestrator) AwaitResume(ctx context.Context, timeout time.Duration) (interface{}, error) {
	if o == nil || o.rt == nil {
		return nil, fmt.Errorf("orchestrator not initialised")
	}
	parent := execution.ContextValue[*execution.Execution](ctx)
	if parent == nil {
		return nil, fmt.Errorf("missing execution in context")
	}
	exec, err := o.rt.WaitForUnblock(ctx, parent.ID, timeout)
	if err != nil {
		return nil, err
	}
	return exec.Output, nil
}

// AwaitGroup blocks until the correlation group completes or timeout elapses.
// It returns the aggregated child outputs.
func (o *Orchestrator) AwaitGroup(ctx context.Context, id string, timeout time.Duration) ([]interface{}, error) {
	if o == nil || o.rt == nil {
		return nil, fmt.Errorf("orchestrator not initialised")
	}
	return o.rt.AwaitGroup(ctx, id, timeout)
}

// Call schedules a single ad-hoc child execution on the shared queue and
// blocks until it completes or timeout elapses. This path does not create a
// correlation group nor switch the parent execution into waitAsync; it is
// ideal for synchronous function-call semantics during LLM streaming.
func (o *Orchestrator) Call(ctx context.Context, child *execution.Execution, timeout time.Duration) (interface{}, error) {
	if o == nil || o.rt == nil {
		return nil, fmt.Errorf("orchestrator not initialised")
	}
	if child == nil {
		return nil, fmt.Errorf("child is nil")
	}
	// Ensure ad-hoc execution semantics so executor can run without a task def.
	child.AtHoc = true
	wait, err := o.rt.ScheduleExecution(ctx, child)
	if err != nil {
		return nil, err
	}
	exec, err := wait(timeout)
	if err != nil {
		return nil, err
	}
	return exec.Output, nil
}
