package execution

import (
	"context"
	"github.com/viant/fluxor/model"
	"github.com/viant/fluxor/model/graph"
	"github.com/viant/fluxor/policy"
	"github.com/viant/fluxor/tracing"
	"sync"
	"time"
)

// RegisterTask adds a task (and its subtasks) to the process' task lookup map
// at runtime.  It is primarily used for template expansions that create tasks
// dynamically after the workflow has started executing.
func (p *Process) RegisterTask(t *graph.Task) {
	if t == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.allTasks == nil {
		p.allTasks = make(map[string]*graph.Task)
	}
	var recurse func(*graph.Task)
	recurse = func(task *graph.Task) {
		if task == nil {
			return
		}
		if _, exists := p.allTasks[task.ID]; !exists {
			p.allTasks[task.ID] = task
			if task.Name != "" {
				p.allTasks[task.Name] = task
			}
		}
		for _, st := range task.Tasks {
			recurse(st)
		}
		if task.Template != nil {
			recurse(task.Template.Task)
		}
	}
	recurse(t)
}

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
	Span       *tracing.Span     `json:"-"`
	Mode       string            `json:"mode"` //debug
	// For serverless environments
	ActiveTaskCount  int                    `json:"activeTaskCount"`
	ActiveTaskGroups map[string]bool        `json:"activeTaskGroups"`
	Policy           *policy.Config         `json:"policy,omitempty"`
	mu               sync.RWMutex           // Protects concurrent access
	allTasks         map[string]*graph.Task // Cached all tasks
}

// setDep safely records taskID dependency state inside e.Dependencies.
func (p *Process) SetDep(e *Execution, taskID string, state TaskState) {
	e.mux.Lock()
	if e.Dependencies == nil {
		e.Dependencies = make(map[string]TaskState)
	}
	e.Dependencies[taskID] = state
	e.mux.Unlock()
}

// getDep safely reads a dependency value; second return value indicates presence.
func (p *Process) GetDep(e *Execution, taskID string) (TaskState, bool) {
	e.mux.RLock()
	val, ok := e.Dependencies[taskID]
	e.mux.RUnlock()
	return val, ok
}

// CopyFrom updates exported, mutex-independent fields from src.  It intentionally
// skips sync.Mutex as copying it would corrupt internal state.
func (p *Process) CopyFrom(src any) {
	other, ok := src.(*Process)
	if !ok || other == nil || p == other {
		return
	}

	// Keep mutex locked while mutating to avoid races.
	p.mu.Lock()
	defer p.mu.Unlock()

	p.SCN = other.SCN
	p.State = other.State
	p.UpdatedAt = other.UpdatedAt
	p.FinishedAt = other.FinishedAt
	p.Stack = other.Stack
	p.Errors = other.Errors
	p.ActiveTaskCount = other.ActiveTaskCount
	p.ActiveTaskGroups = other.ActiveTaskGroups
	// Fields like Session, Workflow, etc. are immutable references – no copy.
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
	if len(p.Stack) == 0 || anExecution == nil {
		return
	}

	// Filter-copy preserving order; this correctly handles removal of any
	// element including the last.
	newStack := p.Stack[:0] // reuse underlying array where possible
	for _, exec := range p.Stack {
		if exec.ID != anExecution.ID {
			newStack = append(newStack, exec)
		}
	}
	// In case every element was removed we still assign an empty slice to
	// avoid holding references.
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

// Clone creates a deep copy of the Process suitable for safe concurrent
// reads/mutations outside the original store.  The Workflow pointer is not
// cloned because workflows are immutable after initial load.
func (p *Process) Clone() *Process {
	if p == nil {
		return nil
	}

	// Copy all scalar and pointer fields except the sync.RWMutex (mu) which
	// must not be copied by value.  Fields that may be mutated after clone are
	// deep-copied to avoid accidental data races.

	out := &Process{
		ID:              p.ID,
		ParentID:        p.ParentID,
		SCN:             p.SCN,
		Name:            p.Name,
		State:           p.State,
		Workflow:        p.Workflow, // immutable – safe to share
		CreatedAt:       p.CreatedAt,
		UpdatedAt:       p.UpdatedAt,
		FinishedAt:      p.FinishedAt,
		Session:         p.Session, // has own locking, safe to share
		Span:            p.Span,
		Mode:            p.Mode,
		ActiveTaskCount: p.ActiveTaskCount,
		Policy:          p.Policy,
		// allTasks intentionally left nil – will be lazily rebuilt if needed
	}

	if len(p.Stack) > 0 {
		out.Stack = make([]*Execution, len(p.Stack))
		for i, ex := range p.Stack {
			out.Stack[i] = ex.Clone()
		}
	}

	if p.Errors != nil {
		out.Errors = make(map[string]string, len(p.Errors))
		for k, v := range p.Errors {
			out.Errors[k] = v
		}
	}

	if p.ActiveTaskGroups != nil {
		out.ActiveTaskGroups = make(map[string]bool, len(p.ActiveTaskGroups))
		for k, v := range p.ActiveTaskGroups {
			out.ActiveTaskGroups[k] = v
		}
	}

	// Preserve dynamically registered tasks so that lookups for template-
	// generated or runtime-added tasks continue to work after the process
	// instance has been cloned (e.g. when it is stored in / loaded from the
	// DAO).  Without this copy, any task added through RegisterTask at runtime
	// would be lost because `allTasks` would be nil in the clone – subsequent
	// allocator iterations would then fail with "task <id> not found".

	if p.allTasks != nil {
		out.allTasks = make(map[string]*graph.Task, len(p.allTasks))
		for k, v := range p.allTasks {
			out.allTasks[k] = v
		}
	}

	return out
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
