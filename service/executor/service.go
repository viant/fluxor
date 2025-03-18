package executor

import (
	"context"
	"fmt"
	"github.com/viant/fluxor/extension"
	"github.com/viant/fluxor/model/execution"
	"github.com/viant/structology/conv"
)

// Service represents a task executor service
type Service interface {
	Execute(ctx context.Context, execution *execution.Execution, process *execution.Process) error
}

// service represents a task executor service implementation
type service struct {
	actions   *extension.Actions
	converter *conv.Converter
}

// Execute executes a task
func (s *service) Execute(ctx context.Context, anExecution *execution.Execution, process *execution.Process) error {

	task := process.LookupTask(anExecution.TaskID)
	if task == nil { //non recoverable error
		return fmt.Errorf("task %s not found in workflow", anExecution.TaskID)
	}
	// Execute task action if defined
	if action := task.Action; action != nil {
		taskService := s.actions.Lookup(task.Action.Service)
		if taskService == nil {
			return fmt.Errorf("service %v not found", task.Action.Service)
		}
		if task.Action.Method == "" {
			return fmt.Errorf("method not found for service %v", task.Action.Service)
		}
		method, err := taskService.Method(task.Action.Method)
		if err != nil {
			return fmt.Errorf("failed to find method %v for service %v: %w", task.Action.Method, task.Action.Service, err)
		}

		session := process.Session.TaskSession(anExecution.Data,
			execution.WithConverter(s.converter),
			execution.WithImports(process.Workflow.Imports...),
			execution.WithTypes(s.actions.Types()))

		imports := process.Workflow.Imports
		if len(imports) == 0 {
			imports = s.actions.Types().Imports()
		}
		if err = session.ApplyParameters(task.Init); err != nil {
			return err
		}

		signature := taskService.Methods().Lookup(task.Action.Method)
		output, err := session.TypedValue(signature.Output, map[string]interface{}{})
		if err != nil {
			return err
		}
		taskInput, err := session.Expand(task.Action.Input)
		if err != nil {
			return err
		}
		input, err := session.TypedValue(signature.Input, taskInput)
		if err != nil {
			return err
		}
		err = method(ctx, input, output)
		anExecution.Output = output
	}

	// Check for transitions to determine the next task to execute
	if len(task.Transitions) > 0 {
		// Evaluate transitions in order
		for _, transition := range task.Transitions {
			// Evaluate condition based on process session state
			conditionMet := evaluateCondition(transition.When, process)
			if conditionMet && transition.Goto != "" {
				anExecution.GoToTask = transition.Goto
				break
			}
		}
	}
	return nil
}

// NewService creates a new executor service
func NewService(actions *extension.Actions) Service {
	options := conv.DefaultOptions()
	options.ClonePointerData = true
	options.IgnoreUnmapped = true
	options.AccessUnexported = true

	return &service{
		converter: conv.NewConverter(options),
		actions:   actions,
	}
}
