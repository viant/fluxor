package execution

import (
	"fmt"
	"github.com/viant/fluxor/model/graph"
	"github.com/viant/fluxor/service/event"
	"time"
)

// Execution represents a single task execution
type Execution struct {
	ID           string                 `json:"id"`
	ProcessID    string                 `json:"processId"`
	ParentTaskID string                 `json:"parentTaskId,omitempty"`
	GroupID      string                 `json:"groupId,omitempty"`
	TaskID       string                 `json:"taskId"`
	State        TaskState              `json:"state"`
	Data         map[string]interface{} `json:"data,omitempty"`
	Input        interface{}            `json:"input,omitempty"`
	Output       interface{}            `json:"empty,omitempty"`
	Error        string                 `json:"error,omitempty"`
	ScheduledAt  time.Time              `json:"scheduledAt"`
	StartedAt    *time.Time             `json:"startedAt,omitempty"`
	PausedAt     *time.Time             `json:"exectedAt,omitempty"`
	CompletedAt  *time.Time             `json:"completedAt,omitempty"`
	GoToTask     string                 `json:"gotoTask,omitempty"`
	Meta         map[string]interface{} `json:"meta,omitempty"`
	DependsOn    []string               `json:"dependencies"`
	Dependencies map[string]TaskState   `json:"completed,omitempty"`
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
	if execution == nil {
		return
	}
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

	if len(e.Dependencies) == 0 {
		e.Dependencies = make(map[string]TaskState)
	}
	for key, value := range execution.Dependencies {
		e.Dependencies[key] = value
	}

	if len(e.Meta) == 0 {
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
	return fmt.Sprintf("%s-%s-%d", processID, taskID, time.Now().UnixNano())
}
