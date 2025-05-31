package model

import (
	"fmt"
	"github.com/viant/fluxor/model/graph"
	"github.com/viant/fluxor/model/state"
	"time"
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

// Validate performs a best-effort structural validation of the workflow.  The
// returned slice is empty when the workflow is sound; otherwise it contains
// human-readable error descriptions.  The function does NOT attempt to execute
// any expressions – it only verifies static properties.
func (w *Workflow) Validate() []error {
	var issues []error

	if w.Pipeline == nil {
		issues = append(issues, fmt.Errorf("pipeline is nil"))
		return issues
	}

	// collect all task IDs
	seen := map[string]bool{}

	var walk func(t *graph.Task)
	walk = func(t *graph.Task) {
		if t == nil {
			return
		}
		if seen[t.ID] {
			issues = append(issues, fmt.Errorf("duplicate task id %s", t.ID))
		}

		seen[t.ID] = true
		seen[t.Name] = true //g

		// validate dependencies refer to existing tasks (so far)
		for _, dep := range t.DependsOn {
			if dep == t.ID {
				issues = append(issues, fmt.Errorf("task %s depends on itself", t.ID))
			}
		}

		for _, st := range t.Tasks {
			walk(st)
		}
	}

	walk(w.Pipeline)
	// Include named dependencies declared at the root level so that tasks within
	// the main pipeline can reference them via `dependsOn`.
	for _, dep := range w.Dependencies {
		walk(dep)
	}

	// After collecting all tasks, verify each dependency / goto exists.
	var check func(*graph.Task)
	check = func(t *graph.Task) {
		if t == nil {
			return
		}
		for _, dep := range t.DependsOn {
			if !seen[dep] {
				issues = append(issues, fmt.Errorf("task %s depends on unknown task %s", t.ID, dep))
			}
		}
		for _, g := range t.Goto {
			if g != nil && g.Task != "" && !seen[g.Task] {
				issues = append(issues, fmt.Errorf("task %s goto refers to unknown task %s", t.ID, g.Task))
			}
		}
		for _, st := range t.Tasks {
			check(st)
		}
	}

	check(w.Pipeline)
	for _, dep := range w.Dependencies {
		check(dep)
	}

	// -----------------------------------------------------------------
	// 3. Detect dependency cycles & unreachable tasks
	// -----------------------------------------------------------------

	// Build adjacency list (dependsOn and sub-task containment)
	edges := map[string][]string{}
	for id, t := range w.AllTasks() {
		// dependsOn edges
		edges[id] = append(edges[id], t.DependsOn...)
		// structural parent → child edges already covered by traversal
	}

	// DFS with colour set (white/grey/black) to detect back-edge cycles
	const (
		white = 0
		grey  = 1
		black = 2
	)
	state := map[string]int{}

	var dfs func(string) bool // returns true if cycle found
	dfs = func(n string) bool {
		st := state[n]
		if st == grey {
			return true // back-edge → cycle
		}
		if st == black {
			return false
		}
		state[n] = grey
		for _, nxt := range edges[n] {
			if dfs(nxt) {
				return true
			}
		}
		state[n] = black
		return false
	}

	if dfs(w.Pipeline.ID) {
		issues = append(issues, fmt.Errorf("workflow contains cyclic dependencies"))
	}

	// Unreachable tasks = tasks that stay white after DFS from root
	for id, col := range state {
		if col == white {
			issues = append(issues, fmt.Errorf("task %s is unreachable from pipeline", id))
		}
	}

	// -----------------------------------------------------------------
	// 4. Template selector sanity checks
	// -----------------------------------------------------------------
	var walkTpl func(*graph.Task)
	walkTpl = func(t *graph.Task) {
		if t == nil {
			return
		}
		if tpl := t.Template; tpl != nil {
			if tpl.Selector == nil || len(*tpl.Selector) == 0 {
				issues = append(issues, fmt.Errorf("task %s has template without selector", t.ID))
			}
		}
		for _, st := range t.Tasks {
			walkTpl(st)
		}
	}
	walkTpl(w.Pipeline)

	// 5. Validate scheduleIn duration strings
	var walkDelay func(*graph.Task)
	walkDelay = func(t *graph.Task) {
		if t == nil {
			return
		}
		if t.ScheduleIn != "" {
			if _, err := time.ParseDuration(t.ScheduleIn); err != nil {
				issues = append(issues, fmt.Errorf("task %s has invalid scheduleIn duration: %v", t.ID, err))
			}
		}
		for _, st := range t.Tasks {
			walkDelay(st)
		}
	}
	walkDelay(w.Pipeline)

	return issues
}

// NewWorkflow creates a new workflow with the given name
func NewWorkflow(name string) *Workflow {
	return &Workflow{
		Name:         name,
		Dependencies: make(map[string]*graph.Task),
	}
}

// WithDescription sets the description of the workflow
func (w *Workflow) WithDescription(description string) *Workflow {
	w.Description = description
	return w
}

// WithVersion sets the version of the workflow
func (w *Workflow) WithVersion(version string) *Workflow {
	w.Version = version
	return w
}

// WithInit adds an initialization parameter to the workflow
func (w *Workflow) WithInit(name string, value interface{}) *Workflow {
	if w.Init == nil {
		w.Init = make(state.Parameters, 0)
	}
	w.Init.Add(name, value)
	return w
}

// WithPost adds a post-execution parameter to the workflow
func (w *Workflow) WithPost(name string, value interface{}) *Workflow {
	if w.Post == nil {
		w.Post = make(state.Parameters, 0)
	}
	w.Post.Add(name, value)
	return w
}

// WithConfig adds a configuration parameter to the workflow
func (w *Workflow) WithConfig(key string, value interface{}) *Workflow {
	if w.Config == nil {
		w.Config = make(map[string]interface{})
	}
	w.Config[key] = value
	return w
}

// WithPipeline sets the main pipeline task for the workflow
func (w *Workflow) WithPipeline(pipeline *graph.Task) *Workflow {
	w.Pipeline = pipeline
	return w
}

// AddDependency adds a dependency task to the workflow
func (w *Workflow) AddDependency(task *graph.Task) *Workflow {
	if w.Dependencies == nil {
		w.Dependencies = make(map[string]*graph.Task)
	}
	w.Dependencies[task.ID] = task
	return w
}

// NewTask creates a new task with the given name and adds it to the workflow pipeline
func (w *Workflow) NewTask(name string) *graph.Task {
	if w.Pipeline == nil {
		w.Pipeline = &graph.Task{
			ID:    w.Name,
			Tasks: make([]*graph.Task, 0),
		}
	}

	task := &graph.Task{
		ID:        w.Pipeline.ID + "/" + name,
		Name:      name,
		Namespace: name,
	}

	w.Pipeline.Tasks = append(w.Pipeline.Tasks, task)
	return task
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
		clone.Pipeline = w.Pipeline.Clone()
	}

	// Clone DependsOn
	if w.Dependencies != nil {
		clone.Dependencies = make(map[string]*graph.Task, len(w.Dependencies))
		for k, v := range w.Dependencies {
			clone.Dependencies[k] = v.Clone()
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
