package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/fluxor/extension"
	"github.com/viant/fluxor/model/execution"
	"github.com/viant/fluxor/model/graph"
	"github.com/viant/fluxor/service/event"
	"github.com/viant/structology/conv"
	"log"
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
	err := s.execute(ctx, anExecution, process, task)
	if err != nil {
		return err
	}

	if value := ctx.Value(execution.EventKey); value != nil {
		service := value.(*event.Service)
		publisher, err := event.PublisherOf[*execution.Execution](service)
		if err == nil {
			task := process.LookupTask(anExecution.TaskID)
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
		var taskInput = task.Action.Input
		if taskInput, err = session.Expand(task.Action.Input); err != nil {
			return err
		}
		input, err := session.TypedValue(signature.Input, taskInput)
		anExecution.Input = input
		if err != nil {
			return err
		}
		err = method(ctx, input, output)

		tt, _ := json.Marshal(task)
		in, _ := json.Marshal(input)
		out, _ := json.Marshal(output)
		fmt.Println(string(tt))
		fmt.Println(string(in))
		fmt.Println(string(out))
		anExecution.Output = output
		if err != nil {
			return err
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
