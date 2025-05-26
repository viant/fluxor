package execution

import (
	"context"
	"github.com/viant/fluxor/extension"
	"github.com/viant/fluxor/model/graph"
	"github.com/viant/fluxor/service/event"
	"reflect"
)

// Context represents the execution context for a process
type Context struct {
	process   *Process
	execution *Execution
	actions   *extension.Actions
	events    *event.Service
	task      *graph.Task
	context.Context
}

var ProcessKey = KeyOf[*Process]()
var ExecutionKey = KeyOf[*Execution]()
var actionsKey = KeyOf[*extension.Actions]()
var EventKey = KeyOf[*event.Service]()
var ContextKey = KeyOf[*Context]()
var TaskKey = KeyOf[*graph.Task]()

// ExecutionContext  returns context with provided process and execution
func (c *Context) ExecutionContext(process *Process, execution *Execution, task *graph.Task) *Context {
	clone := *c
	clone.process = process
	clone.execution = execution
	clone.task = task
	return &clone
}

func (c *Context) Value(key any) any {
	switch key {
	case ProcessKey:
		return c.process
	case ExecutionKey:
		return c.execution
	case actionsKey:
		return c.actions
	case EventKey:
		return c.events
	case ContextKey:
		return c
	case TaskKey:
		return c.task
	}
	return c.Context.Value(key)
}

// ContextValue returns the value of the provided type from the context
func ContextValue[T any](ctx context.Context) T {
	key := KeyOf[T]()
	if value := ctx.Value(key); value != nil {
		return value.(T)
	}
	var t T
	return t
}

// KeyOf returns the reflect.Type of the provided type
func KeyOf[T any]() reflect.Type {
	var a T
	return reflect.TypeOf(a)
}

func NewContext(ctx context.Context, actions *extension.Actions, service *event.Service) *Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return &Context{
		Context: ctx,
		actions: actions,
		events:  service,
	}
}
