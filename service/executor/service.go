package executor

// Package executor implements task execution for Fluxor workflows. The service is able to invoke
// registered extension actions, convert and expand inputs/outputs and, after the user-supplied
// method runs, call an optional listener that can observe the data that flew through the task.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/viant/fluxor/extension"
	"github.com/viant/fluxor/model/execution"
	"github.com/viant/fluxor/model/graph"
	"github.com/viant/fluxor/service/event"
	"github.com/viant/structology/conv"
)

// Listener is invoked once a task action completes (regardless of whether it returned an error or
// not). Implementations can log, collect metrics or perform any other side-effects they require.
//
// For convenience the listener is defined as a function type rather than an interface; users can
// therefore pass a plain function literal when customising the executor.
type Listener func(task *graph.Task, input, output interface{})

// StdoutListener replicates the debug prints that were hard-coded in the previous implementation.
// It serialises the task specification, input and output into JSON and prints them to standard
// output. Errors from json.Marshal are ignored on purpose – they indicate non-serialisable values
// and the caller would not have had access to the data either way in the original implementation.
func StdoutListener(task *graph.Task, input, output interface{}) {
	if task == nil {
		return
	}
	tt, _ := json.Marshal(task)
	fmt.Println(string(tt))
	if task.Action == nil {
		return
	}
	if input != nil {
		in, _ := json.Marshal(input)
		fmt.Println(string(in))
	}

	if output != nil {
		out, _ := json.Marshal(output)
		fmt.Println(string(out))
	}
}

// Option is used to customise the executor instance.
type Option func(*service)

// WithListener overrides the listener invoked after every executed task. Passing nil disables the
// callback entirely.
func WithListener(l Listener) Option {
	return func(s *service) {
		s.listener = l
	}
}

// Service represents a task executor.
type Service interface {
	Execute(ctx context.Context, execution *execution.Execution, process *execution.Process) error
}

// service is the concrete implementation of Service.
type service struct {
	actions   *extension.Actions
	converter *conv.Converter
	listener  Listener
}

// Execute executes a task.
func (s *service) Execute(ctx context.Context, anExecution *execution.Execution, process *execution.Process) error {
	task := process.LookupTask(anExecution.TaskID)
	if task == nil {
		return fmt.Errorf("task %s not found in workflow", anExecution.TaskID)
	}

	// Execute the task action if defined.
	if err := s.execute(ctx, anExecution, process, task); err != nil {
		return err
	}

	// Publish execution event if an event service is attached to the context.
	if value := ctx.Value(execution.EventKey); value != nil {
		service := value.(*event.Service)
		publisher, err := event.PublisherOf[*execution.Execution](service)
		if err == nil {
			eCtx := anExecution.Context("executed", task)
			anEvent := event.NewEvent[*execution.Execution](eCtx, anExecution)
			if err = publisher.Publish(ctx, anEvent); err != nil {
				log.Printf("failed to publish task execution event: %v", err)
			}
		}
	}

	return nil
}

func (s *service) execute(ctx context.Context, anExecution *execution.Execution, process *execution.Process, task *graph.Task) error {
	action := task.Action
	if action == nil {
		// Nothing to execute.
		return nil
	}

	taskService := s.actions.Lookup(action.Service)
	if taskService == nil {
		return fmt.Errorf("service %v not found", action.Service)
	}
	if action.Method == "" {
		return fmt.Errorf("method not found for service %v", action.Service)
	}

	method, err := taskService.Method(action.Method)
	if err != nil {
		return fmt.Errorf("failed to find method %v for service %v: %w", action.Method, action.Service, err)
	}

	// Prepare a task session.
	session := process.Session.TaskSession(anExecution.Data,
		execution.WithConverter(s.converter),
		execution.WithImports(process.Workflow.Imports...),
		execution.WithTypes(s.actions.Types()))

	if err = session.ApplyParameters(task.Init); err != nil {
		return err
	}

	signature := taskService.Methods().Lookup(action.Method)

	output, err := session.TypedValue(signature.Output, map[string]interface{}{})
	if err != nil {
		return err
	}

	taskInput := action.Input
	if taskInput, err = session.Expand(action.Input); err != nil {
		return err
	}

	input, err := session.TypedValue(signature.Input, taskInput)
	anExecution.Input = input
	if err != nil {
		return err
	}

	// Invoke the user-defined method.
	if err = method(ctx, input, output); err != nil {
		return err
	}

	// Call the listener (if any).
	if s.listener != nil {
		s.listener(task, input, output)
	}

	anExecution.Output = output
	return nil
}

// NewService creates a new executor service instance.
//
// The function is backwards-compatible with the previous signature: callers that do not require
// customisation can ignore the variadic options argument.
func NewService(actions *extension.Actions, opts ...Option) Service {
	options := conv.DefaultOptions()
	options.ClonePointerData = true
	options.IgnoreUnmapped = true
	options.AccessUnexported = true

	s := &service{
		actions:   actions,
		converter: conv.NewConverter(options),
		listener:  StdoutListener,
	}

	for _, o := range opts {
		o(s)
	}

	return s
}
