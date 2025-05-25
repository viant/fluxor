package execution

import (
	"context"
	"github.com/viant/fluxor/model"
	"github.com/viant/fluxor/model/graph"
	"sync"
	"time"
)

// Process state constants
const (
	StatePending   = "pending"
	StateRunning   = "running"
	StatePaused    = "paused"
	StateCompleted = "completed"
	StateFailed    = "failed"
)

// Process represents a workflow execution instance
type Process struct {
	ID         string            `json:"id"`
	ParentID   string            `json:"parentId,omitempty"`
	SCN        int               `json:"scn"`
	Name       string            `json:"name"`
	State      string            `json:"state"`
	Workflow   *model.Workflow   `json:"workflow"`
	CreatedAt  time.Time         `json:"createdAt"`
	UpdatedAt  time.Time         `json:"updatedAt"`
	FinishedAt *time.Time        `json:"finishedAt"`
	Session    *Session          `json:"session"`
	Stack      []*Execution      `json:"stack,omitempty"`
	Errors     map[string]string `json:"errors,omitempty"`
	Mode       string            `json:"mode"` //debug
	// For serverless environments
	ActiveTaskCount  int                    `json:"activeTaskCount"`
	ActiveTaskGroups map[string]bool        `json:"activeTaskGroups"`
	mu               sync.RWMutex           // Protects concurrent access
	allTasks         map[string]*graph.Task // Cached all tasks
}

type Wait func(ctx context.Context, timeout time.Duration) (*ProcessOutput, error)

type ProcessOutput struct {
	ProcessID string
	State     string
	Output    map[string]interface{}
	Errors    map[string]string
	TimeTaken time.Duration
	Timeout   bool
}

func (p *Process) LookupTask(taskID string) *graph.Task {
	allTasks := p.AllTasks()
	return allTasks[taskID]
}

func (p *Process) LookupExecution(taskID string) *Execution {
	for i := len(p.Stack) - 1; i >= 0; i-- {
		if p.Stack[i].TaskID == taskID {
			return p.Stack[i]
		}
	}
	return nil
}

func (p *Process) AllTasks() map[string]*graph.Task {
	p.mu.RLock()
	ret := p.allTasks
	p.mu.RUnlock()
	if ret != nil {
		return ret
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.allTasks = p.Workflow.AllTasks()
	return p.allTasks
}

// NewProcess creates a new process
func NewProcess(id string, name string, workflow *model.Workflow, initialState map[string]interface{}) *Process {
	now := time.Now()
	if initialState == nil {
		initialState = make(map[string]interface{})
	}
	return &Process{
		ID:               id,
		Name:             name,
		State:            StatePending,
		Workflow:         workflow,
		CreatedAt:        now,
		UpdatedAt:        now,
		Session:          NewSession(id, WithState(initialState)),
		ActiveTaskCount:  0,
		ActiveTaskGroups: make(map[string]bool),
		Errors:           make(map[string]string),
	}
}

func (p *Process) Push(executions ...*Execution) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Stack = append(p.Stack, executions...)
}

func (p *Process) Remove(anExecution *Execution) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.Stack) == 0 {
		return
	}
	var newStack []*Execution
	for i := 0; i < len(p.Stack)-1; i++ {
		if p.Stack[i].ID != anExecution.ID {
			newStack = append(newStack, p.Stack[i])
		}
	}
	p.Stack = newStack
}

func (p *Process) Peek() *Execution {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.Stack) == 0 {
		return nil
	}
	execution := p.Stack[len(p.Stack)-1]
	return execution
}

// GetState returns the process state
func (p *Process) GetState() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.State
}

// SetState updates the process state
func (p *Process) SetState(state string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.State = state
	switch state {
	case StateCompleted:
		now := time.Now()
		p.FinishedAt = &now
	case StateFailed:
		now := time.Now()
		p.FinishedAt = &now
	case StatePaused:
		// Do nothing
	}
	p.UpdatedAt = time.Now()
}

// IncrementActiveTaskCount increments the active task counter
func (p *Process) IncrementActiveTaskCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ActiveTaskCount++
	return p.ActiveTaskCount
}

// DecrementActiveTaskCount decrements the active task counter
func (p *Process) DecrementActiveTaskCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ActiveTaskCount > 0 {
		p.ActiveTaskCount--
	}
	return p.ActiveTaskCount
}

// GetActiveTaskCount returns the current active task count
func (p *Process) GetActiveTaskCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.ActiveTaskCount
}

// AddActiveTaskGroup marks a task group as active
func (p *Process) AddActiveTaskGroup(groupID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ActiveTaskGroups[groupID] = true
}

// RemoveActiveTaskGroup removes a task group from active groups
func (p *Process) RemoveActiveTaskGroup(groupID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.ActiveTaskGroups, groupID)
}

// HasActiveTaskGroup checks if a task group is active
func (p *Process) HasActiveTaskGroup(groupID string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, exists := p.ActiveTaskGroups[groupID]
	return exists
}
