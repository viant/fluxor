package executor

// Package executor implements task execution for Fluxor workflows. The service is able to invoke
// registered extension actions, convert and expand inputs/outputs and, after the user-supplied
// method runs, call an optional listener that can observe the data that flew through the task.

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/fluxor/service/approval"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/viant/fluxor/extension"
	"github.com/viant/fluxor/model/graph"
	"github.com/viant/fluxor/policy"
	"github.com/viant/fluxor/runtime/execution"
	"github.com/viant/fluxor/service/event"
	"github.com/viant/fluxor/tracing"
	"github.com/viant/structology/conv"
)

// Listener is invoked once a task action completes (regardless of whether it returned an error or
// not). Implementations can log, collect metrics or perform any other side-effects they require.
//
// For convenience the listener is defined as a function type rather than an interface; users can
// therefore pass a plain function literal when customising the executor.
type Listener func(task *graph.Task, exec *execution.Execution)

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

// WithApprovalService applies the approval service to the executor. The approval service is used to
func WithApprovalService(approval approval.Service) Option {
	return func(s *service) {
		s.approval = approval
	}
}

// WithListener overrides the listener invoked after every executed task. Passing nil disables the
// callback entirely.
func WithListener(l Listener) Option {
	return func(s *service) {
		s.listener = l
	}
}

// WithApprovalSkipPrefixes configures the executor so that actions whose fully
// qualified name (service.method, lower-cased) starts with any of the
// specified prefixes will bypass the approval request in policy ModeAsk.
func WithApprovalSkipPrefixes(prefixes ...string) Option {
	lower := make([]string, 0, len(prefixes))
	for _, p := range prefixes {
		if p == "" {
			continue
		}
		lower = append(lower, strings.ToLower(p))
	}
	return func(s *service) {
		s.skipPrefixes = append(s.skipPrefixes, lower...)
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
	approval  approval.Service

	// skipPrefixes defines service/method prefixes (lower-case, e.g.
	// "system.storage" or "printer.") that are executed automatically even
	// when the workflow policy mode is "ask" – no approval request is
	// generated.
	skipPrefixes []string
}

// Execute executes a task.
func (s *service) Execute(ctx context.Context, anExecution *execution.Execution, process *execution.Process) error {
	task := process.LookupTask(anExecution.TaskID)
	if anExecution.AtHoc {
		task = anExecution.AtHocTask()
	}
	if task == nil {
		return ErrTaskNotFound
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

func (s *service) execute(ctx context.Context, anExecution *execution.Execution, aProcess *execution.Process, task *graph.Task) error {
	action := task.Action
	if action == nil {
		// Nothing to execute.
		return nil
	}

	// ------------------------------------------------------------------
	// OpenTelemetry span for the task action
	// ------------------------------------------------------------------
	spanName := fmt.Sprintf("task.execute %s.%s", action.Service, action.Method)
	ctx, span := tracing.StartSpan(ctx, spanName, "INTERNAL")
	span.WithAttributes(map[string]string{
		"execution.id": anExecution.ID,
		"aProcess.id":  anExecution.ProcessID,
		"task.id":      anExecution.TaskID,
		"service":      action.Service,
		"method":       action.Method,
	})

	var spanErr error
	defer func(start time.Time) {
		// record duration as attribute (ms)
		durMs := time.Since(start).Milliseconds()
		span.WithAttributes(map[string]string{"duration.ms": fmt.Sprint(durMs)})
		tracing.EndSpan(span, spanErr)
	}(time.Now())

	taskService := s.actions.Lookup(action.Service)
	if taskService == nil {
		spanErr = fmt.Errorf("service %v not found", action.Service)
		return spanErr
	}
	if action.Method == "" {
		spanErr = ErrMethodNotFound
		return spanErr
	}

	// ------------------------------------------------------------------
	// Per-action policy hook (ask/auto/deny) – opt-in via context.
	// ------------------------------------------------------------------
	// Evaluate persisted policy declaration (approval/deny lists)
	var pol *policy.Policy
	if aProcess != nil && aProcess.Policy != nil {
		pol = policy.FromConfig(aProcess.Policy)
	}

	if pol != nil {
		actionName := strings.ToLower(action.Service)
		if action.Method != "" {
			actionName += "." + strings.ToLower(action.Method)
		}

		// Allow / block lists first.
		if !pol.IsAllowed(actionName) {
			return fmt.Errorf("action %s is not allowed by policy", actionName)
		}

		switch pol.Mode {
		case policy.ModeDeny:
			return fmt.Errorf("action %s denied by policy", actionName)
		case policy.ModeAsk:
			// If the execution has already been approved or rejected, honour the
			// previous decision instead of creating a new request. This prevents an
			// infinite request → approval → re-request loop.
			if anExecution.Approved != nil {
				if *anExecution.Approved {
					// Previously approved – continue and run the action just like in
					// ModeAuto.
					break
				}

				// Previously rejected – fail fast, include the user-supplied reason
				reason := anExecution.ApprovalReason
				if reason == "" {
					reason = "rejected by user"
				}
				return fmt.Errorf("action %s rejected: %s", actionName, reason)
			}

			// ------------------------------------------------------------------
			// No decision recorded yet – create an asynchronous approval request
			// and pause execution until we get the answer.
			// ------------------------------------------------------------------

			// Executor-level override: some actions (e.g. internal logging) may be
			// deemed safe and therefore bypass approval even when the workflow
			// Policy is in ask mode.
			if s.shouldSkipApproval(actionName) {
				break // continue with automatic execution
			}

			// Build a concrete, expanded copy of the action input so that the
			// approval request contains the real values rather than template
			// placeholders.

			if s.approval != nil {
				// Start tracing span for approval request
				ctxSpan, sp := tracing.StartSpan(ctx, fmt.Sprintf("approval.request %s", actionName), "INTERNAL")
				_ = ctxSpan
				sp.WithAttributes(map[string]string{
					"execution.id": anExecution.ID,
					"action":       actionName,
				})

				// Attempt to expand any placeholders using the same logic that will be
				// applied during actual execution after the approval arrives.  This
				// gives reviewers full visibility into the concrete arguments rather
				// than the raw template.
				expandedArgs := action.Input
				sess := aProcess.Session.TaskSession(anExecution.Data,
					execution.WithConverter(s.converter),
					execution.WithImports(aProcess.Workflow.Imports...),
					execution.WithTypes(s.actions.Types()))
				if v, err := sess.Expand(action.Input); err == nil {
					expandedArgs = v
				}

				data, _ := json.Marshal(expandedArgs)
				// Use execution ID as the unique identifier for the approval request
				// unless the caller supplies another value. This guarantees that every
				// wait-for-approval state generates a retrievable request so that the
				// approval service can later match it when Decide is invoked.
				req := &approval.Request{
					ID:          anExecution.ID,
					ProcessID:   anExecution.ProcessID,
					ExecutionID: anExecution.ID,
					Action:      actionName,
					Args:        data,
					CreatedAt:   time.Now(),
				}
				_ = s.approval.RequestApproval(ctx, req)
				tracing.EndSpan(sp, nil)
			}

			anExecution.State = execution.TaskStateWaitForApproval
			anExecution.Approved = nil
			return nil
		}
	}

	method, err := taskService.Method(action.Method)
	if err != nil {
		spanErr = fmt.Errorf("failed to find method %v for service %v: %w", action.Method, action.Service, err)
		return spanErr
	}

	// Prepare a task session.
	session := aProcess.Session.TaskSession(anExecution.Data,
		execution.WithConverter(s.converter),
		execution.WithImports(aProcess.Workflow.Imports...),
		execution.WithTypes(s.actions.Types()))

	if err = session.ApplyParameters(task.Init); err != nil {
		spanErr = err
		return spanErr
	}

	signature := taskService.Methods().Lookup(action.Method)

	output, err := session.TypedValue(signature.Output, map[string]interface{}{})
	if err != nil {
		spanErr = err
		return spanErr
	}
	if signature.Output.Kind() == reflect.Struct && signature.Output.NumField() == 0 {
		var m = [1]interface{}{}
		output = &m[0]
	}

	taskInput := anExecution.Input
	if taskInput == nil {
		taskInput = action.Input
		if taskInput, err = session.Expand(action.Input); err != nil {
			spanErr = err
			return spanErr
		}
	}
	input, err := session.TypedValue(signature.Input, taskInput)
	anExecution.Input = input
	if err != nil {
		spanErr = err
		return spanErr
	}

	// Invoke the user-defined method.
	err = method(ctx, input, output)
	// Normalise Output: when the declared output type is interface{}, we pass
	// a pointer to interface{} to the method. Replace it with the concrete
	// value to avoid leaking a pointer wrapper into the rest of the engine.
	if p, ok := output.(*interface{}); ok && p != nil {
		anExecution.Output = *p
	} else {
		anExecution.Output = output
	}
	if err != nil {
		anExecution.Error = err.Error()
		spanErr = err
	}

	// Call the listener (if any).
	if s.listener != nil {
		s.listener(task, anExecution)
	}

	return err
}

// shouldSkipApproval returns true when actionName (already lower-cased) starts
// with any configured prefix.
func (s *service) shouldSkipApproval(actionName string) bool {
	if len(s.skipPrefixes) == 0 {
		return false
	}
	for _, p := range s.skipPrefixes {
		if strings.HasPrefix(actionName, p) {
			return true
		}
	}
	return false
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
	}
	for _, o := range opts {
		o(s)
	}

	return s
}
