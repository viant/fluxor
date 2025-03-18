package model

import (
	"github.com/viant/fluxor/model/graph"
	"github.com/viant/fluxor/model/state"
)

// Workflow represents a workflow definition
type Workflow struct {

	// Source provides information about the origin of the workflow
	Source *Source `json:"source,omitempty" yaml:"source,omitempty"`
	// Name is the unique identifier for the workflow
	Name string `json:"name" yaml:"name"`

	// Description provides a human-readable description of the workflow
	Description string `json:"description,omitempty" yaml:"description,omitempty"`

	TypeName string `json:"typeName,omitempty" yaml:"typeName,omitempty"`

	// Imports represents a collection of package imports
	Imports Imports

	// Version specifies the workflow version
	Version string `json:"version,omitempty" yaml:"version,omitempty"`

	// Init parameters are applied at the beginning of workflow execution
	Init state.Parameters `json:"init,omitempty" yaml:"init,omitempty"`

	// Pipeline defines the main execution graph of the workflow
	Pipeline *graph.Task `json:"pipeline,omitempty" yaml:"pipeline,omitempty"`

	// Dependencies define reusable tasks that can be referenced by ID
	Dependencies map[string]*graph.Task `json:"dependencies,omitempty" yaml:"dependencies,omitempty"`

	// Post parameters are applied at the end of workflow execution
	Post state.Parameters `json:"post,omitempty" yaml:"post,omitempty"`

	// Config contains workflow-level configuration
	Config map[string]interface{} `json:"config,omitempty" yaml:"config,omitempty"`

	AutoPause *bool `json:"autoPause,omitempty" yaml:"autoPause,omitempty"`
}

// Import represents a package import
type Import struct {
	Package string `json:"package,omitempty" yaml:"package,omitempty"`
	PkgPath string `json:"pkgPath,omitempty" yaml:"pkgPath,omitempty"`
}

// Imports represents a collection of package imports
type Imports []*Import

func (i Imports) IndexByPackage() map[string]*Import {
	result := make(map[string]*Import)
	for _, item := range i {
		result[item.Package] = item
	}
	return result
}

func (i Imports) IsUnique() bool {
	var unique = make(map[string]bool)
	for _, item := range i {
		if _, unknown := unique[item.Package]; unknown {
			return false
		}
		unique[item.Package] = true
	}
	return len(unique) == len(i)
}

func (i Imports) PkgPath(pkg string) string {
	for _, item := range i {
		if item.Package == pkg {
			return item.PkgPath
		}
	}
	return ""
}

func (i Imports) HasPkgPath(pkgPath string) bool {
	for _, item := range i {
		if item.PkgPath == pkgPath {
			return true
		}
	}
	return false
}

// AllTasks returns all tasks in the workflow
func (w *Workflow) AllTasks() map[string]*graph.Task {
	tasks := make(map[string]*graph.Task)
	w.traverseTask(w.Pipeline, tasks)
	for _, task := range w.Dependencies {
		w.traverseTask(task, tasks)
	}
	return tasks
}

// traverseTask recursively traverses the task and its subtasks
func (w *Workflow) traverseTask(task *graph.Task, tasks map[string]*graph.Task) {
	if task == nil {
		return
	}
	if _, exists := tasks[task.ID]; !exists {
		tasks[task.ID] = task
		tasks[task.Name] = task
		for _, subtask := range task.Tasks {
			w.traverseTask(subtask, tasks)
		}
	}
}

type Source struct {
	URL string `json:"url,omitempty" yaml:"url,omitempty"`
}

// Clone creates a deep copy of the workflow
func (w *Workflow) Clone() *Workflow {
	if w == nil {
		return nil
	}

	clone := &Workflow{
		Name:        w.Name,
		Description: w.Description,
		Version:     w.Version,
	}

	// Clone Init parameters
	if w.Init != nil {
		clone.Init = make(state.Parameters, len(w.Init))
		copy(clone.Init, w.Init)
	}

	// Clone Pipeline
	if w.Pipeline != nil {
		clone.Pipeline = cloneTask(w.Pipeline)
	}

	// Clone DependsOn
	if w.Dependencies != nil {
		clone.Dependencies = make(map[string]*graph.Task, len(w.Dependencies))
		for k, v := range w.Dependencies {
			clone.Dependencies[k] = cloneTask(v)
		}
	}

	// Clone Post parameters
	if w.Post != nil {
		clone.Post = make(state.Parameters, len(w.Post))
		copy(clone.Post, w.Post)
	}

	// Clone Config
	if w.Config != nil {
		clone.Config = make(map[string]interface{}, len(w.Config))
		for k, v := range w.Config {
			clone.Config[k] = v
		}
	}

	return clone
}

// cloneTask creates a deep copy of a task
func cloneTask(task *graph.Task) *graph.Task {
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

	// Clone DependsOn
	if task.DependsOn != nil {
		clone.DependsOn = make([]string, len(task.DependsOn))
		copy(clone.DependsOn, task.DependsOn)
	}

	// Clone Init parameters
	if task.Init != nil {
		clone.Init = make(state.Parameters, len(task.Init))
		copy(clone.Init, task.Init)
	}

	// Clone Action
	if task.Action != nil {
		clone.Action = &graph.Action{
			Service: task.Action.Service,
			Method:  task.Action.Method,
			Input:   task.Action.Input,
		}
	}

	// Clone Tasks recursively
	if task.Tasks != nil {
		clone.Tasks = make([]*graph.Task, len(task.Tasks))
		for i, subtask := range task.Tasks {
			clone.Tasks[i] = cloneTask(subtask)
		}
	}

	// Clone Post parameters
	if task.Post != nil {
		clone.Post = make(state.Parameters, len(task.Post))
		copy(clone.Post, task.Post)
	}

	// Clone Template
	if task.Template != nil {
		clone.Template = &graph.Template{
			Task:     cloneTask(task.Template.Task),
			Selector: task.Template.Selector,
		}
	}

	// Clone Transitions
	if task.Transitions != nil {
		clone.Transitions = make([]*graph.Transition, len(task.Transitions))
		for i, transition := range task.Transitions {
			clone.Transitions[i] = &graph.Transition{
				When: transition.When,
				Goto: transition.Goto,
			}
		}
	}

	return clone
}
