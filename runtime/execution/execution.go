package execution

import (
	"fmt"
	"github.com/viant/fluxor/internal/idgen"
	"github.com/viant/fluxor/model/graph"
	"github.com/viant/fluxor/service/event"
	"sync"
	"time"
)

// Execution represents a single task execution
type Execution struct {
	ID             string                 `json:"id"`
	ProcessID      string                 `json:"processId"`
	ParentTaskID   string                 `json:"parentTaskId,omitempty"`
	GroupID        string                 `json:"groupId,omitempty"`
	TaskID         string                 `json:"taskId"`
	State          TaskState              `json:"state"`
	Data           map[string]interface{} `json:"data,omitempty"`
	Input          interface{}            `json:"input,omitempty"`
	Output         interface{}            `json:"empty,omitempty"`
	Error          string                 `json:"error,omitempty"`
	Attempts       int                    `json:"attempts,omitempty"`
	ScheduledAt    time.Time              `json:"scheduledAt"`
	StartedAt      *time.Time             `json:"startedAt,omitempty"`
	PausedAt       *time.Time             `json:"exectedAt,omitempty"`
	CompletedAt    *time.Time             `json:"completedAt,omitempty"`
	GoToTask       string                 `json:"gotoTask,omitempty"`
	Meta           map[string]interface{} `json:"meta,omitempty"`
	RunAfter       *time.Time             `json:"runAfter,omitempty"`
	DependsOn      []string               `json:"dependencies"`
	Dependencies   map[string]TaskState   `json:"completed,omitempty"`
	mux            sync.RWMutex           `json:"-"`
	Approved       *bool                  `json:"approved,omitempty"`
	ApprovalReason string                 `json:"approvedDecision,omitempty"` // "yes" or "no"
}

func (e *Execution) Context(eventType string, task *graph.Task) *event.Context {
	ret := &event.Context{
		EventType: eventType,
		ProcessID: e.ProcessID,
		TaskID:    e.TaskID,
	}
	if action := task.Action; action != nil {
		ret.Service = action.Service
		ret.Method = action.Method
	}
	return ret

}

// NewExecution creates a new execution for a task
func NewExecution(processID string, parent, task *graph.Task) *Execution {
	ret := &Execution{
		ID:           generateExecutionID(processID, task.ID),
		ProcessID:    processID,
		TaskID:       task.ID,
		State:        TaskStatePending,
		ScheduledAt:  time.Now(),
		DependsOn:    task.DependsOn,
		Dependencies: make(map[string]TaskState),
	}

	// Initialize dependencies map with all dependencies and subtasks
	for _, subTask := range task.Tasks {
		ret.Dependencies[subTask.ID] = TaskStatePending
	}

	for _, dependency := range task.DependsOn {
		ret.Dependencies[dependency] = TaskStatePending
	}

	if parent != nil {
		ret.ParentTaskID = parent.ID
		if parent.Async {
			ret.GroupID = parent.ID
		}
	}

	return ret
}

// Start marks the execution as started
func (e *Execution) Start() {
	now := time.Now()
	e.StartedAt = &now
	e.State = TaskStateRunning
}

// Complete marks the execution as completed
func (e *Execution) Complete() {
	now := time.Now()
	e.CompletedAt = &now
	e.State = TaskStateCompleted
}

func (e *Execution) Pause() {
	t := time.Now()
	e.PausedAt = &t
	e.State = TaskStatePaused
}

// Fail marks the execution as failed
func (e *Execution) Fail(err error) {
	now := time.Now()
	e.CompletedAt = &now
	if err != nil {
		e.Error = err.Error()
	}
	e.State = TaskStateFailed
}

func (e *Execution) Schedule() {
	now := time.Now()
	e.ScheduledAt = now
}

func (e *Execution) Merge(execution *Execution) {
	if execution == nil || execution == e {
		return
	}
	e.mux.Lock()
	execution.mux.RLock()
	defer execution.mux.RUnlock()
	defer e.mux.Unlock()

	if execution.Output != nil {
		e.Output = execution.Output
	}
	if execution.GoToTask != "" {
		e.GoToTask = execution.GoToTask
	}
	if execution.State != "" {
		e.State = execution.State
	}
	if execution.Error != "" {
		e.Error = execution.Error
	}
	if execution.StartedAt != nil {
		e.StartedAt = execution.StartedAt
	}
	if execution.CompletedAt != nil {
		e.CompletedAt = execution.CompletedAt
	}
	if execution.PausedAt != nil {
		e.PausedAt = execution.PausedAt
	}

	if e.Dependencies == nil {
		e.Dependencies = make(map[string]TaskState)
	}
	for key, value := range execution.Dependencies {
		e.Dependencies[key] = value
	}

	if e.Meta == nil {
		e.Meta = make(map[string]interface{})
	}
	for key, value := range execution.Meta {
		e.Meta[key] = value
	}
}

func (e *Execution) Skip() {
	e.State = TaskStateSkipped
}

// generateExecutionID creates a unique ID for an execution
func generateExecutionID(processID, taskID string) string {
	return fmt.Sprintf("%s-%s-%s", processID, taskID, idgen.New())
}

// Clone creates a deep copy of the execution so that the caller can mutate it
// without affecting the original instance.  Only mutable collections are
// deep-copied; pointer fields referencing immutable data (Input / Output /
// Workflow structures) are left as-is.
func (e *Execution) Clone() *Execution {
	if e == nil {
		return nil
	}
	e.mux.RLock()
	defer e.mux.RUnlock()

	clone := *e // shallow copy primitives & pointers (includes mux contents)
	// re-initialise the mutex so the copy has its own lock independent from
	// the source.
	clone.mux = sync.RWMutex{}

	if e.Data != nil {
		clone.Data = make(map[string]interface{}, len(e.Data))
		for k, v := range e.Data {
			clone.Data[k] = v
		}
	}

	if e.Meta != nil {
		clone.Meta = make(map[string]interface{}, len(e.Meta))
		for k, v := range e.Meta {
			clone.Meta[k] = v
		}
	}

	if e.Dependencies != nil {
		clone.Dependencies = make(map[string]TaskState, len(e.Dependencies))
		for k, v := range e.Dependencies {
			clone.Dependencies[k] = v
		}
	}

	if len(e.DependsOn) > 0 {
		clone.DependsOn = append([]string(nil), e.DependsOn...)
	}

	if e.RunAfter != nil {
		t := *e.RunAfter
		clone.RunAfter = &t
	}

	return &clone
}
