package graph

import (
	"github.com/viant/fluxor/model/state"
)

type (
	Action struct {
		Service string      `json:"service,omitempty" yaml:"service,omitempty"`
		Method  string      `json:"method,omitempty" yaml:"method,omitempty"`
		Input   interface{} `json:"input,omitempty" yaml:"input,omitempty"`
	}

	Task struct {
		ID        string           `json:"id,omitempty" yaml:"id,omitempty"`
		TypeName  string           `json:"typeName,omitempty" yaml:"typeName,omitempty"`
		Name      string           `json:"name,omitempty" yaml:"name,omitempty"`
		Namespace string           `json:"namespace,omitempty" yaml:"namespace,omitempty"`
		Init      state.Parameters `json:"init,omitempty" yaml:"init,omitempty"`
		When      string           `json:"when,omitempty" yaml:"when,omitempty"`
		Action    *Action          `json:"action,omitempty" yaml:"action,omitempty"`
		DependsOn []string         `json:"dependsOn,omitempty" yaml:"dependsOn,omitempty"`
		Tasks     []*Task          `json:"tasks,omitempty" yaml:"tasks,omitempty"`
		Post      state.Parameters `json:"post,omitempty" yaml:"post,omitempty"`
		Template  *Template        `json:"template,omitempty" yaml:"template,omitempty"`
		Goto      []*Transition    `json:"goto,omitempty" yaml:"goto,omitempty"`
		Async     bool             `json:"async,omitempty" yaml:"async,omitempty"`
		AutoPause *bool            `json:"autoPause,omitempty" yaml:"autoPause,omitempty"`
		Retry     *Retry           `json:"retry,omitempty" yaml:"retry,omitempty"`
	}

	// Retry strategy for task
	Retry struct {
		Type       string  `json:"type,omitempty" yaml:"type,omitempty"` // fixed, exponential, none
		MaxRetries int     `json:"maxRetries,omitempty" yaml:"maxRetries,omitempty"`
		Delay      string  `json:"delay,omitempty" yaml:"delay,omitempty"`           // base delay (duration string)
		Multiplier float64 `json:"multiplier,omitempty" yaml:"multiplier,omitempty"` // exponential multiplier (>1)
		MaxDelay   string  `json:"maxDelay,omitempty" yaml:"maxDelay,omitempty"`
	}

	Template struct {
		Task     *Task             `json:"task,omitempty" yaml:"task,omitempty"`
		Selector *state.Parameters `json:"selector,omitempty" yaml:"selector,omitempty"`
	}

	Transition struct {
		When string `json:"when,omitempty" yaml:"when,omitempty"`
		Task string `json:"task,omitempty" yaml:"task,omitempty"`
	}
)

func (t *Task) IsAsync() bool {
	return t.Async
}

func (t *Task) IsAutoPause() bool {
	if t.AutoPause == nil {
		return false
	}
	return *t.AutoPause
}

// WithAction sets the action for the task
func (t *Task) WithAction(service string, method string, input interface{}) *Task {
	t.Action = &Action{
		Service: service,
		Method:  method,
		Input:   input,
	}
	return t
}

// WithInit adds an initialization parameter to the task
func (t *Task) WithInit(name string, value interface{}) *Task {
	if t.Init == nil {
		t.Init = make(state.Parameters, 0)
	}
	t.Init.Add(name, value)
	return t
}

// WithPost adds a post-execution parameter to the task
func (t *Task) WithPost(name string, value interface{}) *Task {
	if t.Post == nil {
		t.Post = make(state.Parameters, 0)
	}
	t.Post.Add(name, value)
	return t
}

// WithDependsOn adds a dependency to the task
func (t *Task) WithDependsOn(taskID string) *Task {
	if t.DependsOn == nil {
		t.DependsOn = make([]string, 0)
	}
	t.DependsOn = append(t.DependsOn, taskID)
	return t
}

// WithGoto adds a transition to the task
func (t *Task) WithGoto(when string, taskName string) *Task {
	if t.Goto == nil {
		t.Goto = make([]*Transition, 0)
	}
	t.Goto = append(t.Goto, &Transition{
		When: when,
		Task: taskName,
	})
	return t
}

// WithAsync sets the task to run asynchronously
func (t *Task) WithAsync(async bool) *Task {
	t.Async = async
	return t
}

// WithAutoPause sets the auto-pause flag for the task
func (t *Task) WithAutoPause(autoPause bool) *Task {
	t.AutoPause = &autoPause
	return t
}

// AddSubTask adds a subtask to the task
func (t *Task) AddSubTask(name string) *Task {
	if t.Tasks == nil {
		t.Tasks = make([]*Task, 0)
	}

	subtask := &Task{
		ID:        t.ID + "/" + name,
		Name:      name,
		Namespace: name,
	}

	t.Tasks = append(t.Tasks, subtask)
	return subtask
}

// CreateSubTask creates a subtask with the given properties and adds it to the task
func (t *Task) CreateSubTask(name string, options ...func(*Task) *Task) *Task {
	subtask := t.AddSubTask(name)

	// Apply all the provided options to the subtask
	for _, option := range options {
		subtask = option(subtask)
	}

	return subtask
}

// Clone creates a deep copy of a task
func (t *Task) Clone() *Task {
	if t == nil {
		return nil
	}

	clone := &Task{
		ID:        t.ID,
		Name:      t.Name,
		Namespace: t.Namespace,
		When:      t.When,
		Async:     t.Async,
	}

	// Clone DependsOn
	if t.DependsOn != nil {
		clone.DependsOn = make([]string, len(t.DependsOn))
		copy(clone.DependsOn, t.DependsOn)
	}

	// Clone Init parameters
	if t.Init != nil {
		clone.Init = make(state.Parameters, len(t.Init))
		copy(clone.Init, t.Init)
	}

	// Clone Action
	if t.Action != nil {
		clone.Action = &Action{
			Service: t.Action.Service,
			Method:  t.Action.Method,
			Input:   t.Action.Input,
		}
	}

	// Clone Tasks recursively
	if t.Tasks != nil {
		clone.Tasks = make([]*Task, len(t.Tasks))
		for i, subtask := range t.Tasks {
			clone.Tasks[i] = subtask.Clone()
		}
	}

	// Clone Post parameters
	if t.Post != nil {
		clone.Post = make(state.Parameters, len(t.Post))
		copy(clone.Post, t.Post)
	}

	// Clone Template
	if t.Template != nil {
		clone.Template = &Template{
			Task:     t.Template.Task.Clone(),
			Selector: t.Template.Selector,
		}
	}

	// Clone Goto
	if t.Goto != nil {
		clone.Goto = make([]*Transition, len(t.Goto))
		for i, transition := range t.Goto {
			clone.Goto[i] = &Transition{
				When: transition.When,
				Task: transition.Task,
			}
		}
	}
	return clone
}

/*

// cloneGraphTask creates a deep copy of a graph.Task, including nested tasks, templates, and transitions.
func cloneGraphTask(task *graph.Task) *graph.Task {
   if task == nil {
       return nil
   }
   clone := &graph.Task{
       ID:        task.ID,
       Name:      task.Name,
       Namespace: task.Namespace,
       When:      task.When,
       Async:     task.Async,
   }
   if task.DependsOn != nil {
       clone.DependsOn = append([]string{}, task.DependsOn...)
   }
   if task.Init != nil {
       clone.Init = make(state.Parameters, len(task.Init))
       copy(clone.Init, task.Init)
   }
   if task.Action != nil {
       clone.Action = &graph.Action{
           Service: task.Action.Service,
           Method:  task.Action.Method,
           Input:   task.Action.Input,
       }
   }
   if task.Tasks != nil {
       clone.Tasks = make([]*graph.Task, len(task.Tasks))
       for i, t := range task.Tasks {
           clone.Tasks[i] = t.Clone()
       }
   }
   if task.Post != nil {
       clone.Post = make(state.Parameters, len(task.Post))
       copy(clone.Post, task.Post)
   }
   if task.Template != nil {
       clone.Template = &graph.Template{
           Task:     task.Template.Task.Clone(),
           Selector: task.Template.Selector,
       }
   }
   if task.Goto != nil {
       clone.Goto = make([]*graph.Transition, len(task.Goto))
       for i, tr := range task.Goto {
           clone.Goto[i] = &graph.Transition{
               When: tr.When,
               Task: tr.Task,
           }
       }
   }
   return clone
}
*/
