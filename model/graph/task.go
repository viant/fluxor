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
		ID          string           `json:"id,omitempty" yaml:"id,omitempty"`
		TypeName    string           `json:"typeName,omitempty" yaml:"typeName,omitempty"`
		Name        string           `json:"name,omitempty" yaml:"name,omitempty"`
		Namespace   string           `json:"namespace,omitempty" yaml:"namespace,omitempty"`
		Init        state.Parameters `json:"init,omitempty" yaml:"init,omitempty"`
		When        string           `json:"when,omitempty" yaml:"when,omitempty"`
		Action      *Action          `json:"action,omitempty" yaml:"action,omitempty"`
		DependsOn   []string         `json:"dependsOn,omitempty" yaml:"dependsOn,omitempty"`
		Tasks       []*Task          `json:"tasks,omitempty" yaml:"tasks,omitempty"`
		Post        state.Parameters `json:"post,omitempty" yaml:"post,omitempty"`
		Template    *Template        `json:"template,omitempty" yaml:"template,omitempty"`
		Transitions []*Transition    `json:"transitions,omitempty" yaml:"transitions,omitempty"`
		Async       bool             `json:"async,omitempty" yaml:"async,omitempty"`
		AutoPause   *bool            `json:"autoPause,omitempty" yaml:"autoPause,omitempty"`
	}

	Template struct {
		*Task
		Selector string `json:"selector,omitempty" yaml:"selector,omitempty"`
	}

	Transition struct {
		When string `json:"when,omitempty" yaml:"when,omitempty"`
		Goto string `json:"goto,omitempty" yaml:"goto,omitempty"`
	}
)

func (t *Task) IsAutoPause() bool {
	if t.AutoPause == nil {
		return false
	}
	return *t.AutoPause
}
