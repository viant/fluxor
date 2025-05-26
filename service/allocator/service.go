package allocator

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/fluxor/model/graph"
	execution2 "github.com/viant/fluxor/runtime/execution"
	"github.com/viant/fluxor/runtime/expander"
	"github.com/viant/fluxor/service/dao"
	"github.com/viant/fluxor/service/event"
	"github.com/viant/fluxor/service/messaging"
	"github.com/viant/fluxor/tracing"
	"log"
	"reflect"
	"strings"
	"time"
)

// toInterfaceSlice converts an array or slice value to a []interface{}.
func toInterfaceSlice(raw interface{}) ([]interface{}, error) {
	v := reflect.ValueOf(raw)
	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		return nil, fmt.Errorf("expected slice or array, got %T", raw)
	}
	length := v.Len()
	result := make([]interface{}, length)
	for i := 0; i < length; i++ {
		result[i] = v.Index(i).Interface()
	}
	return result, nil
}

// addDynamicTask adds a cloned task and its subtasks to the process's task map
func addDynamicTask(allTasks map[string]*graph.Task, task *graph.Task) {
	if task == nil {
		return
	}
	if _, exists := allTasks[task.ID]; exists {
		return
	}
	allTasks[task.ID] = task
	if task.Name != "" {
		allTasks[task.Name] = task
	}
	// traverse subtasks
	for _, sub := range task.Tasks {
		addDynamicTask(allTasks, sub)
	}
	// include nested template subgraph
	if task.Template != nil {
		addDynamicTask(allTasks, task.Template.Task)
	}
}

// Config represents allocator service configuration
type Config struct {
	// PollingInterval is how often the allocator checks for processes that need tasks
	PollingInterval time.Duration
}

// DefaultConfig returns the default allocator configuration
func DefaultConfig() Config {
	return Config{
		PollingInterval: 20 * time.Millisecond,
	}
}

// Service allocates tasks to processes
type Service struct {
	config           Config
	processDAO       dao.Service[string, execution2.Process]
	taskExecutionDao dao.Service[string, execution2.Execution]
	queue            messaging.Queue[execution2.Execution]
	shutdownCh       chan struct{}
}

// New creates a new allocator service
func New(processDAO dao.Service[string, execution2.Process], taskExecutionDao dao.Service[string, execution2.Execution], queue messaging.Queue[execution2.Execution], config Config) *Service {
	return &Service{
		config:           config,
		processDAO:       processDAO,
		taskExecutionDao: taskExecutionDao,
		queue:            queue,
		shutdownCh:       make(chan struct{}),
	}
}

// Start begins the task allocation loop
func (s *Service) Start(ctx context.Context) error {
	ticker := time.NewTicker(s.config.PollingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.shutdownCh:
			return nil
		case <-ticker.C:
			if err := s.allocateTasks(ctx); err != nil {
				// Log error but continue
				fmt.Printf("Error allocating tasks: %v\n", err)
			}
		}
	}
}

// handleTransition processDAO a goto transition by creating a new execution path
func (s *Service) handleTransition(ctx context.Context, processID string, fromTaskID string, toTaskID string) error {
	aProcess, err := s.processDAO.Load(ctx, processID)
	if err != nil {
		return err
	}
	allTasks := aProcess.Workflow.AllTasks()
	parentTask := allTasks[fromTaskID]
	nextTask := allTasks[toTaskID]
	nextExecution := execution2.NewExecution(processID, parentTask, nextTask)
	aProcess.Push(nextExecution)
	// Save the process with the updated plan
	return s.processDAO.Save(ctx, aProcess)
	// Let the allocator schedule the next task
}

// allocateTasks finds processes that need tasks and allocates them
func (s *Service) allocateTasks(ctx context.Context) error {
	// Get all processes
	processes, err := s.processDAO.List(ctx, dao.NewParameter("State", execution2.StatePending, execution2.StateRunning))
	if err != nil {
		return fmt.Errorf("failed to list processes: %w", err)
	}

	// Process each running workflow
	for _, process := range processes {
		// Skip processes that aren't running
		if process.GetState() != execution2.StateRunning {
			continue
		}

		// Process doesn't have maximum active tasks, scheduleTask next tasks
		if err := s.scheduleNextTasks(ctx, process); err != nil {
			// Log but continue with other processes
			fmt.Printf("Error scheduling tasks for process %s: %v\n", process.ID, err)
			continue
		}
	}

	return nil
}

// scheduleNextTasks allocates the next ready tasks for a process
func (s *Service) scheduleNextTasks(ctx context.Context, process *execution2.Process) error {
	// Skip if process has reached max concurrent tasks

	// Check if there are tasks on the stack to execute
	if len(process.Stack) == 0 {
		// No more tasks to execute, check if process is complete
		if process.GetState() == execution2.StateRunning {
			if len(process.Errors) > 0 {
				process.SetState(execution2.StateFailed)
			} else {
				process.SetState(execution2.StateCompleted)
			}
			// End process-level span if present
			if process.Span != nil {
				var endErr error
				if process.GetState() == execution2.StateFailed {
					endErr = fmt.Errorf("process failed with %d errors", len(process.Errors))
				}
				tracing.EndSpan(process.Span, endErr)
				process.Span = nil
			}
			return s.processDAO.Save(ctx, process)
		}
		return nil
	}

	// Get the top execution from the stack
	anExecution := process.Peek()
	var err error
	currentTask := process.LookupTask(anExecution.TaskID)
	switch anExecution.State {
	case execution2.TaskStatePending:
		done, err := s.handlePendingTask(ctx, process, currentTask, anExecution)
		if err != nil {
			return s.handleExecutionError(ctx, process, anExecution, err)
		}

		if done {
			return nil
		}
	case execution2.TaskStateRunning, execution2.TaskStateScheduled, execution2.TaskStatePaused:
		scheduleSubTasks, err := s.handleRunningTask(ctx, process, anExecution)
		if !scheduleSubTasks || err != nil {
			return err
		}
	}

	dependencyState, err := s.ensureDependencies(ctx, process, anExecution)
	if err != nil || dependencyState == execution2.TaskStateRunning {
		return err
	}

	switch dependencyState {
	case execution2.TaskStateFailed:
		return s.handleProcessedExecution(ctx, process, anExecution, dependencyState)
	}

	switch anExecution.State {
	case execution2.TaskStateWaitForDependencies, execution2.TaskStatePending:
		if currentTask.Action != nil {
			if err := s.updateExecutionState(ctx, process, anExecution, execution2.TaskStateScheduled); err != nil {
				return fmt.Errorf("failed to update execution state: %w", err)
			}
			return nil
		}
	}

	status, err := s.ensureSubTasks(ctx, process, anExecution)
	if err != nil {
		return err
	}
	switch status {
	case execution2.TaskStateRunning:
		return nil
	default:
		return s.handleProcessedExecution(ctx, process, anExecution, status)
	}
}

func (s *Service) handlePendingTask(ctx context.Context, process *execution2.Process, currentTask *graph.Task, anExecution *execution2.Execution) (bool, error) {
	var err error
	// Evaluate conditional execution
	if currentTask.When != "" {
		canRun, err := evaluateCondition(currentTask.When, process, currentTask, anExecution, true)
		if err != nil {
			return true, err
		}
		if !canRun {
			anExecution.Skip()
			return true, s.handleProcessedExecution(ctx, process, anExecution, anExecution.State)
		}
	}
	// Handle templated (repeat) tasks
	if currentTask.Template != nil {
		// Selector must be present
		selParams := currentTask.Template.Selector
		if selParams == nil || len(*selParams) == 0 {
			return true, fmt.Errorf("template selector is empty for task %s", currentTask.ID)
		}
		// Expand the first selector into a slice/array
		first := (*selParams)[0]
		raw, err := process.Session.Expand(first.Value)
		if err != nil {
			return true, fmt.Errorf("failed to expand template selector for task %s: %w", currentTask.ID, err)
		}
		items, err := toInterfaceSlice(raw)
		if err != nil {
			return true, fmt.Errorf("template selector for task %s must be slice or array: %w", currentTask.ID, err)
		}
		// For each element, clone the template subgraph and create an execution
		allTasks := process.AllTasks()
		for idx, item := range items {
			// Deep-clone the template task tree
			clone := currentTask.Template.Task.Clone()
			// Assign a unique ID for the clone
			clone.ID = fmt.Sprintf("%s[%d]", currentTask.ID, idx)
			// Prepare execution with element-bound data
			exec := execution2.NewExecution(process.ID, currentTask, clone)
			exec.Data = make(map[string]interface{})
			// Bind selector parameters: first => element, others => index or expanded
			for si, param := range *selParams {
				name := param.Name
				if si == 0 {
					exec.Data[name] = item
				} else if str, ok := param.Value.(string); ok && str == "$i" {
					exec.Data[name] = idx
				} else {
					// fallback expand any other selector value
					if val, _ := process.Session.Expand(param.Value); val != nil {
						exec.Data[name] = val
					}
				}
			}
			// Push the new execution on the stack
			process.Push(exec)
			// register cloned task in lookup map
			addDynamicTask(allTasks, clone)
		}
		// Mark the driver task as waiting for subtasks
		if err := s.updateExecutionState(ctx, process, anExecution, execution2.TaskStateWaitForSubTasks); err != nil {
			return true, err
		}
		return true, nil
	}

	// Normal task: preserve existing Data (e.g. from template) and apply Init params
	if anExecution.Data == nil {
		anExecution.Data = make(map[string]interface{})
	}
	for _, parameter := range currentTask.Init {
		if _, exists := anExecution.Data[parameter.Name]; !exists {
			if val, expErr := process.Session.Expand(parameter.Value); expErr != nil {
				return true, expErr
			} else {
				anExecution.Data[parameter.Name] = val
			}
		}
	}
	return false, err
}

func (s *Service) handleRunningTask(ctx context.Context, process *execution2.Process, anExecution *execution2.Execution) (bool, error) {
	runningExecution, err := s.taskExecutionDao.Load(ctx, anExecution.ID)
	if err != nil {
		return false, fmt.Errorf("failed to load running execution: %w", err)
	}
	if runningExecution.State == anExecution.State {
		return false, nil
	}
	anExecution.Merge(runningExecution)
	switch runningExecution.State {
	case execution2.TaskStateRunning, execution2.TaskStatePaused:
		return false, nil
	case execution2.TaskStateCompleted:
	case execution2.TaskStateFailed, execution2.TaskStateSkipped:
		return false, s.handleProcessedExecution(ctx, process, anExecution, runningExecution.State)
	}
	return true, nil
}

func (s *Service) ensureDependencies(ctx context.Context, process *execution2.Process, anExecution *execution2.Execution) (execution2.TaskState, error) {
	completed := 0
	currentTask := process.LookupTask(anExecution.TaskID)

	var scheduled []*execution2.Execution
	// Check if all dependencies are satisfied
outer:
	for _, depID := range anExecution.DependsOn {
		task := process.LookupTask(depID)
		status := anExecution.Dependencies[task.ID]
		switch status {
		case execution2.TaskStatePending:
			scheduled = append(scheduled, execution2.NewExecution(process.ID, currentTask, task))
			anExecution.Dependencies[task.ID] = execution2.TaskStateScheduled
			break outer
		case execution2.TaskStateCompleted, execution2.TaskStateSkipped:
			completed++
		case execution2.TaskStateFailed:
			return execution2.TaskStateFailed, nil
		}
	}
	if len(scheduled) > 0 {
		process.Push(scheduled...)
		if err := s.updateExecutionState(ctx, process, anExecution, execution2.TaskStateWaitForDependencies); err != nil {
			return execution2.TaskStateFailed, fmt.Errorf("failed to update execution state: %w", err)
		}
	}
	dependenciesMet := len(anExecution.DependsOn) == completed
	if dependenciesMet {
		return execution2.StateCompleted, nil
	}
	return execution2.StateRunning, nil
}

func (s *Service) ensureSubTasks(ctx context.Context, process *execution2.Process, anExecution *execution2.Execution) (execution2.TaskState, error) {
	completed := 0
	currentTask := process.LookupTask(anExecution.TaskID)

	async := false

	if anExecution.ParentTaskID != "" {
		if parent := process.LookupTask(anExecution.ParentTaskID); parent != nil {
			async = parent.Async
		}
	}

	var scheduled []*execution2.Execution
	// Check if all dependencies are satisfied
outer:
	for i := range currentTask.Tasks {
		task := currentTask.Tasks[i]
		status := anExecution.Dependencies[task.ID]
		switch status {
		case execution2.TaskStatePending:
			scheduled = append(scheduled, execution2.NewExecution(process.ID, currentTask, task))
			anExecution.Dependencies[task.ID] = execution2.TaskStateScheduled
			if !async {
				break outer
			}
		case execution2.TaskStateFailed:
			return execution2.TaskStateFailed, nil
		case execution2.TaskStateCompleted, execution2.TaskStateSkipped:
			completed++
		}
	}
	if len(scheduled) > 0 {
		process.Push(scheduled...)
		if err := s.updateExecutionState(ctx, process, anExecution, execution2.TaskStateWaitForSubTasks); err != nil {
			return execution2.TaskStateFailed, fmt.Errorf("failed to update execution state: %w", err)
		}
	}
	subTaskCompleted := len(currentTask.Tasks) == completed
	if subTaskCompleted {
		return execution2.TaskStateCompleted, nil
	}
	return execution2.TaskStateRunning, nil
}

func (s *Service) updateExecutionState(ctx context.Context, process *execution2.Process, anExecution *execution2.Execution, state execution2.TaskState) error {
	if state == execution2.TaskStateScheduled {
		anExecution.Schedule()
	}
	anExecution.State = state
	if err := s.taskExecutionDao.Save(ctx, anExecution); err != nil {
		return fmt.Errorf("failed to save execution: %w", err)
	}
	err := s.processDAO.Save(ctx, process)
	if err != nil {
		return fmt.Errorf("failed to save process: %w", err)
	}
	switch state {
	case execution2.TaskStateScheduled:
		if err = s.publishTaskExecution(ctx, process, anExecution); err != nil {
			return fmt.Errorf("failed to publish task execution: %w", err)
		}
	}
	return nil
}

func (s *Service) scheduleTask(ctx context.Context, process *execution2.Process, parentTaskId, taskId string, anExecution *execution2.Execution) *execution2.Execution {
	parent := process.LookupTask(parentTaskId)
	task := process.LookupTask(taskId)
	status := anExecution.Dependencies[task.ID]
	if status == execution2.TaskStatePending {
		depExecution := execution2.NewExecution(process.ID, parent, task)
		anExecution.Dependencies[task.ID] = execution2.TaskStateScheduled
		process.Push(depExecution)
		return depExecution
	}
	return nil
}

// publishTaskExecution creates and publishes an execution for a single task
func (s *Service) publishTaskExecution(ctx context.Context, process *execution2.Process, anExecution *execution2.Execution) error {
	if value := ctx.Value(execution2.EventKey); value != nil {
		service := value.(*event.Service)
		publisher, err := event.PublisherOf[*execution2.Execution](service)
		if err == nil {
			task := process.LookupTask(anExecution.TaskID)
			eCtx := anExecution.Context("scheduled", task)
			anEvent := event.NewEvent[*execution2.Execution](eCtx, anExecution)
			if err = publisher.Publish(ctx, anEvent); err != nil {
				log.Printf("failed to publish task execution event: %v", err)
			}
		}
	}
	return s.queue.Publish(ctx, anExecution)
}

// TaskCompleted marks a task as completed for a process
func (s *Service) TaskCompleted(ctx context.Context, processID string, taskID string) error {
	// Decrement the active task count for this process

	// Load the process
	process, err := s.processDAO.Load(ctx, processID)
	if err != nil {
		return fmt.Errorf("failed to load process %s: %w", processID, err)
	}

	anExecution := process.LookupExecution(taskID)
	parentExecution := process.LookupExecution(anExecution.ParentTaskID)
	if parentExecution != nil {
		parentExecution.Dependencies[taskID] = execution2.TaskStateCompleted
	}
	// Save the updated process
	if err := s.processDAO.Save(ctx, process); err != nil {
		return fmt.Errorf("failed to save process %s: %w", processID, err)
	}

	// Immediately try to scheduleTask more tasks
	return s.scheduleNextTasks(ctx, process)
}

// Shutdown stops the allocator service
func (s *Service) Shutdown() {
	close(s.shutdownCh)
}

func (s *Service) handleProcessedExecution(ctx context.Context, process *execution2.Process, anExecution *execution2.Execution, state execution2.TaskState) error {
	currentTask := process.LookupTask(anExecution.TaskID)

	if state == execution2.TaskStateCompleted {
		output := anExecution.Output
		var outputMap = make(map[string]interface{})
		if data, err := json.Marshal(anExecution.Output); err == nil {
			if err = json.Unmarshal(data, &outputMap); err == nil {
				output = outputMap
			}
		}

		process.Session.Set(currentTask.Namespace, output)
		err := s.handleTaskDone(currentTask, process, anExecution, outputMap)
		if err != nil {
			return err
		}
	}
	if anExecution.Error != "" {
		process.Errors[currentTask.Namespace] = anExecution.Error
	}
	parent := process.LookupExecution(anExecution.ParentTaskID)
	if parent != nil {
		parent.Dependencies[anExecution.TaskID] = state
	}
	process.Remove(anExecution)
	err := s.processDAO.Save(ctx, process)
	if err != nil {
		return fmt.Errorf("failed to save process: %w", err)
	}
	if anExecution.GoToTask != "" {
		return s.handleTransition(ctx, process.ID, anExecution.TaskID, anExecution.GoToTask)
	}
	return nil
}

func (s *Service) handleTaskDone(currentTask *graph.Task, process *execution2.Process, anExecution *execution2.Execution, outputMap map[string]interface{}) error {

	source := process.Session.Clone()
	for k, v := range outputMap {
		source.Set(k, v)
	}

	for _, parameter := range currentTask.Post {
		evaluated, err := expander.Expand(parameter.Value, source.State)
		if err == nil {
			name := parameter.Name
			isAppend := strings.HasSuffix(name, "[]")
			if isAppend {
				process.Session.Append(strings.TrimRight(name, "[]"), evaluated)
				continue
			}
			process.Session.Set(parameter.Name, evaluated)
		}
	}
	if len(currentTask.Goto) > 0 {
		// Evaluate transitions in order
		for _, transition := range currentTask.Goto {
			// Evaluate condition based on process session state
			conditionMet, err := evaluateCondition(transition.When, process, currentTask, anExecution, false)
			if err != nil {
				return err
			}
			if conditionMet && transition.Task != "" {
				anExecution.GoToTask = transition.Task
				break
			}
		}
	}
	return nil

}

func (s *Service) handleExecutionError(ctx context.Context, process *execution2.Process, anExecution *execution2.Execution, err error) error {
	anExecution.Error += err.Error()
	return s.handleProcessedExecution(ctx, process, anExecution, execution2.TaskStateFailed)
}
