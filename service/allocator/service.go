package allocator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/viant/fluxor/tracing"
	"log"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/viant/fluxor/model/graph"
	"github.com/viant/fluxor/progress"
	execution "github.com/viant/fluxor/runtime/execution"
	"github.com/viant/fluxor/runtime/expander"
	"github.com/viant/fluxor/service/dao"
	"github.com/viant/fluxor/service/event"
	"github.com/viant/fluxor/service/messaging"
)

func findCatchTask(parent *graph.Task) *graph.Task {
	if parent == nil {
		return nil
	}
	for _, st := range parent.Tasks {
		if strings.EqualFold(st.Name, "catch") {
			return st
		}
	}
	return nil
}

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

// keysSnapshot returns the sorted list of keys present in the provided map.
func keysSnapshot(m map[string]*graph.Task) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	if len(out) > 20 {
		return out[:20] // limit length for readability
	}
	return out
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
	processDAO       dao.Service[string, execution.Process]
	taskExecutionDao dao.Service[string, execution.Execution]
	queue            messaging.Queue[execution.Execution]
	shutdownCh       chan struct{}
	// serialize scheduling to prevent overlapping allocation passes
	scheduleMu sync.Mutex
}

// New creates a new allocator service
func New(processDAO dao.Service[string, execution.Process], taskExecutionDao dao.Service[string, execution.Execution], queue messaging.Queue[execution.Execution], config Config) *Service {
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
	nextExecution := execution.NewExecution(processID, parentTask, nextTask)
	aProcess.Push(nextExecution)
	// Save the process with the updated plan
	return s.processDAO.Save(ctx, aProcess)
	// Let the allocator schedule the next task
}

// allocateTasks finds processes that need tasks and allocates them
func (s *Service) allocateTasks(ctx context.Context) error {
	// Get all processes
	processes, err := s.processDAO.List(ctx, dao.NewParameter("State", execution.StatePending, execution.StateRunning, execution.StateCancelRequested))
	if err != nil {
		return fmt.Errorf("failed to list processes: %w", err)
	}

	// Process each running workflow
	for _, process := range processes {
		// Skip processes that aren't running
		state := process.GetState()
		if state == execution.StateCancelRequested {
			// If there are no active tasks left we can mark the process as cancelled.
			if len(process.Stack) == 0 || process.GetActiveTaskCount() == 0 {
				process.SetState(execution.StateCancelled)
				_ = s.processDAO.Save(ctx, process)
			}
			continue // never schedule more work
		}
		if state != execution.StateRunning {
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
func (s *Service) scheduleNextTasks(ctx context.Context, process *execution.Process) error {
	// serialize scheduling for this process to prevent overlapping passes
	s.scheduleMu.Lock()
	defer s.scheduleMu.Unlock()
	// Check if there are tasks on the stack to execute
	if len(process.Stack) == 0 {
		// No more tasks to execute, check if process is complete
		if process.GetState() == execution.StateRunning {
			if len(process.Errors) > 0 {
				process.SetState(execution.StateFailed)
			} else {
				process.SetState(execution.StateCompleted)
			}
			// End process-level span if present
			if process.Span != nil {
				var endErr error
				if process.GetState() == execution.StateFailed {
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

	//THIS need to be moved somewhere else - if you cancel execution there from peek it would never to be handled
	if anExecution.RunAfter != nil && time.Now().Before(*anExecution.RunAfter) {
		return nil
	}
	var err error
	currentTask := process.LookupTask(anExecution.TaskID)
	if currentTask == nil {
		fmt.Printf("LookupTask failed for %s. existing keys: %v\n", anExecution.TaskID, keysSnapshot(process.AllTasks()))
		return fmt.Errorf("task %s not found in workflow", anExecution.TaskID)
	}
	switch anExecution.State {
	case execution.TaskStatePending:
		done, err := s.handlePendingTask(ctx, process, currentTask, anExecution)
		if err != nil {
			return s.handleExecutionError(ctx, process, anExecution, err)
		}

		if done {
			return nil
		}
	case execution.TaskStateRunning, execution.TaskStateScheduled, execution.TaskStatePaused:
		scheduleSubTasks, err := s.handleRunningTask(ctx, process, anExecution)
		if !scheduleSubTasks || err != nil {
			return err
		}
	}

	dependencyState, err := s.ensureDependencies(ctx, process, anExecution)
	if err != nil || dependencyState == execution.TaskStateRunning {
		return err
	}

	switch dependencyState {
	case execution.TaskStateFailed:
		return s.handleProcessedExecution(ctx, process, anExecution, dependencyState)
	}

	switch anExecution.State {
	case execution.TaskStateWaitForDependencies, execution.TaskStatePending:
		if currentTask.Action != nil {
			if err := s.updateExecutionState(ctx, process, anExecution, execution.TaskStateScheduled); err != nil {
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
	case execution.TaskStateRunning:
		return nil
	default:
		return s.handleProcessedExecution(ctx, process, anExecution, status)
	}
}

func (s *Service) handlePendingTask(ctx context.Context, process *execution.Process, currentTask *graph.Task, anExecution *execution.Execution) (bool, error) {
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
		for idx, item := range items {
			// Deep-clone the template task tree
			clone := currentTask.Template.Task.Clone()
			// Assign a unique ID for the clone
			clone.ID = fmt.Sprintf("%s[%d]", currentTask.ID, idx)
			// Prepare execution with element-bound data
			exec := execution.NewExecution(process.ID, currentTask, clone)
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
			// register cloned task in lookup map (thread-safe)
			process.RegisterTask(clone)
		}
		// Mark the driver task as waiting for subtasks
		if err := s.updateExecutionState(ctx, process, anExecution, execution.TaskStateWaitForSubTasks); err != nil {
			return true, err
		}
		return true, nil
	}

	// If task has scheduleIn and this is the first time we reach scheduling
	if currentTask.ScheduleIn != "" && anExecution.RunAfter == nil {
		if d, err := time.ParseDuration(currentTask.ScheduleIn); err == nil {
			runAt := time.Now().Add(d)
			anExecution.RunAfter = &runAt
		} else {
			return true, fmt.Errorf("invalid scheduleIn duration on task %s: %v", currentTask.ID, err)
		}
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

func (s *Service) handleRunningTask(ctx context.Context, process *execution.Process, anExecution *execution.Execution) (bool, error) {
	runningExecution, err := s.taskExecutionDao.Load(ctx, anExecution.ID)
	if err != nil {
		if errors.Is(err, dao.ErrNotFound) {
			return true, nil
		}
		return false, fmt.Errorf("failed to load running execution: %w", err)
	}
	if runningExecution.State == anExecution.State {
		return false, nil
	}
	anExecution.Merge(runningExecution)
	switch runningExecution.State {
	case execution.TaskStateRunning, execution.TaskStatePaused:
		return false, nil
	case execution.TaskStateCompleted:
	case execution.TaskStateFailed, execution.TaskStateSkipped:
		return false, s.handleProcessedExecution(ctx, process, anExecution, runningExecution.State)
	}
	return true, nil
}

func (s *Service) ensureDependencies(ctx context.Context, process *execution.Process, anExecution *execution.Execution) (execution.TaskState, error) {
	completed := 0
	currentTask := process.LookupTask(anExecution.TaskID)

	// Guard against inconsistent state that may occur when the execution was
	// created before dynamic sub-tasks (e.g. from template or goto) got
	// registered.  Ensure we always have a map to index.
	if anExecution.Dependencies == nil {
		anExecution.Dependencies = make(map[string]execution.TaskState)
	}

	var scheduled []*execution.Execution
	// Check if all dependencies are satisfied
outer:
	for _, depID := range anExecution.DependsOn {
		task := process.LookupTask(depID)
		status := s.taskState(ctx, process, anExecution, task)
		switch status {
		case execution.TaskStatePending:
			scheduled = append(scheduled, execution.NewExecution(process.ID, currentTask, task))
			process.SetDependencyState(anExecution, task.ID, execution.TaskStateScheduled)
			break outer
		case execution.TaskStateCompleted, execution.TaskStateSkipped:
			completed++
		case execution.TaskStateFailed:
			return execution.TaskStateFailed, nil
		}
	}
	if len(scheduled) > 0 {
		process.Push(scheduled...)
		if err := s.updateExecutionState(ctx, process, anExecution, execution.TaskStateWaitForDependencies); err != nil {
			return execution.TaskStateFailed, fmt.Errorf("failed to update execution state: %w", err)
		}
	}
	dependenciesMet := len(anExecution.DependsOn) == completed
	if dependenciesMet {
		return execution.StateCompleted, nil
	}
	return execution.StateRunning, nil
}

func (s *Service) ensureSubTasks(ctx context.Context, process *execution.Process, anExecution *execution.Execution) (execution.TaskState, error) {
	completed := 0
	currentTask := process.LookupTask(anExecution.TaskID)

	async := false

	if anExecution.ParentTaskID != "" {
		if parent := process.LookupTask(anExecution.ParentTaskID); parent != nil {
			async = parent.Async
		}
	}

	// load persisted execution to skip already-scheduled subtasks
	var persistedExec *execution.Execution
	if persistedProc, err := s.processDAO.Load(ctx, process.ID); err == nil {
		persistedExec = persistedProc.LookupExecution(anExecution.TaskID)
	} else if !errors.Is(err, dao.ErrNotFound) {
		return execution.TaskStateFailed, fmt.Errorf(
			"failed to load process for idempotent scheduling check: %w", err,
		)
	}
	var scheduled []*execution.Execution
	// Check if all dependencies are satisfied and schedule only still-pending subtasks
outer:
	for i := range currentTask.Tasks {
		task := currentTask.Tasks[i]
		status := s.taskState(ctx, process, anExecution, task)
		switch status {
		case execution.TaskStatePending:
			// skip if persisted dependency state already moved off Pending
			if persistedExec != nil {
				if state, ok := persistedExec.Dependencies[task.ID]; ok && state != execution.TaskStatePending {
					continue
				}
			}
			scheduled = append(scheduled, execution.NewExecution(process.ID, currentTask, task))
			process.SetDependencyState(anExecution, task.ID, execution.TaskStateScheduled)
			if !async {
				break outer
			}
		case execution.TaskStateFailed:
			return execution.TaskStateFailed, nil
		case execution.TaskStateCompleted, execution.TaskStateSkipped:
			completed++
		}
	}
	if len(scheduled) > 0 {
		process.Push(scheduled...)
		if err := s.updateExecutionState(ctx, process, anExecution, execution.TaskStateWaitForSubTasks); err != nil {
			return execution.TaskStateFailed, fmt.Errorf("failed to update execution state: %w", err)
		}
	}
	subTaskCompleted := len(currentTask.Tasks) == completed
	if subTaskCompleted {
		return execution.TaskStateCompleted, nil
	}
	return execution.TaskStateRunning, nil
}

func (s *Service) taskState(ctx context.Context, process *execution.Process, parentExecution *execution.Execution, task *graph.Task) execution.TaskState {
	status, _ := process.DependencyState(parentExecution, task.ID)
	if anExecution, _ := s.taskExecutionDao.Load(ctx, task.ID); anExecution != nil {
		status = anExecution.State
	}
	return status
}

func (s *Service) updateExecutionState(ctx context.Context, process *execution.Process, anExecution *execution.Execution, state execution.TaskState) error {
	if state == execution.TaskStateScheduled {
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
	case execution.TaskStateScheduled:
		if err = s.publishTaskExecution(ctx, process, anExecution); err != nil {
			return fmt.Errorf("failed to publish task execution: %w", err)
		}
	}
	return nil
}

func (s *Service) scheduleTask(ctx context.Context, process *execution.Process, parentTaskId, taskId string, anExecution *execution.Execution) *execution.Execution {
	parent := process.LookupTask(parentTaskId)
	task := process.LookupTask(taskId)
	status := s.taskState(ctx, process, anExecution, task)
	if status == execution.TaskStatePending {
		depExecution := execution.NewExecution(process.ID, parent, task)
		process.SetDependencyState(anExecution, task.ID, execution.TaskStateScheduled)
		process.Push(depExecution)
		return depExecution
	}
	return nil
}

// publishTaskExecution creates and publishes an execution for a single task
func (s *Service) publishTaskExecution(ctx context.Context, process *execution.Process, anExecution *execution.Execution) error {
	if value := ctx.Value(execution.EventKey); value != nil {
		service := value.(*event.Service)
		publisher, err := event.PublisherOf[*execution.Execution](service)
		if err == nil {
			task := process.LookupTask(anExecution.TaskID)
			eCtx := anExecution.Context("scheduled", task)
			anEvent := event.NewEvent[*execution.Execution](eCtx, anExecution)
			if err = publisher.Publish(ctx, anEvent); err != nil {
				log.Printf("failed to publish task execution event: %v", err)
			}
		}
	}
	return s.queue.Publish(ctx, anExecution)
}

// TaskCompleted marks a task as completed for a process

// Shutdown stops the allocator service
func (s *Service) Shutdown() {
	close(s.shutdownCh)
}

func (s *Service) handleProcessedExecution(ctx context.Context, process *execution.Process, anExecution *execution.Execution, state execution.TaskState) error {
	// Update progress counters based on final state of this execution.
	var delta progress.Delta
	parent := process.LookupExecution(anExecution.ParentTaskID)
	if parent != nil {
		process.SetDependencyState(parent, anExecution.TaskID, state)
	}

	switch state {
	case execution.TaskStateCompleted:
		delta = progress.Delta{Running: -1, Completed: +1}
	case execution.TaskStateFailed:
		delta = progress.Delta{Running: -1, Failed: +1}
	case execution.TaskStateSkipped:
		delta = progress.Delta{Running: -1, Skipped: +1}
	default:
		delta = progress.Delta{Running: -1}
	}
	progress.UpdateCtx(process.Ctx, delta)
	currentTask := process.LookupTask(anExecution.TaskID)
	if state == execution.TaskStateCompleted && currentTask.Namespace != "" {
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
	//	parent := process.LookupExecution(anExecution.ParentTaskID)
	if parent != nil {
		// if child failed schedule parent catch task if any
		if state == execution.TaskStateFailed {
			if catchTask := findCatchTask(process.LookupTask(parent.TaskID)); catchTask != nil {
				depState := s.taskState(ctx, process, parent, catchTask)
				if depState == "" || depState == execution.TaskStatePending {
					newExec := execution.NewExecution(process.ID, process.LookupTask(parent.TaskID), catchTask)
					process.SetDependencyState(parent, catchTask.ID, execution.TaskStateScheduled)
					process.Push(newExec)
					process.Session.Set("_lastError", anExecution.Error)
					process.Session.Set("_lastErrorTask", anExecution.TaskID)
					process.Session.Set("_lastErrorProcess", anExecution.ProcessID)
				}
			}
		}

		// If the task triggered a goto jump we skip all remaining sibling tasks
		// (those that come after the current one in the parent's task list) so
		// they are not executed in this iteration.
		if anExecution.GoToTask != "" {
			parentTask := process.LookupTask(parent.TaskID)
			if parentTask != nil {
				idx := -1
				for i, st := range parentTask.Tasks {
					if st.ID == anExecution.TaskID {
						idx = i
						break
					}
				}
				if idx >= 0 {
					for i := idx + 1; i < len(parentTask.Tasks); i++ {
						sib := parentTask.Tasks[i]
						process.SetDependencyState(parent, sib.ID, execution.TaskStateSkipped)
					}
				}
			}
		}
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

func (s *Service) handleTaskDone(currentTask *graph.Task, process *execution.Process, anExecution *execution.Execution, outputMap map[string]interface{}) error {

	source := process.Session.Clone()
	for k, v := range outputMap {
		source.Set(k, v)
	}

	for _, parameter := range currentTask.Post {
		evaluated, err := expander.Expand(parameter.Value, source.State)
		if err != nil {
			return fmt.Errorf("failed to expand post parameter %s: %w", parameter.Name, err)
		}
		name := parameter.Name
		isAppend := strings.HasSuffix(name, "[]")
		if isAppend {
			process.Session.Append(strings.TrimRight(name, "[]"), evaluated)
			continue
		}
		process.Session.Set(parameter.Name, evaluated)
	}

	if len(currentTask.Goto) > 0 {
		// Evaluate transitions in order
		for _, transition := range currentTask.Goto {
			conditionMet := true
			if transition.When != "" {
				var err error
				// Evaluate condition based on process session state
				conditionMet, err = evaluateCondition(transition.When, process, currentTask, anExecution, false)
				if err != nil {
					return err
				}
			}
			if conditionMet && transition.Task != "" {
				anExecution.GoToTask = transition.Task
				break
			}
		}
	}
	return nil

}

func (s *Service) handleExecutionError(ctx context.Context, process *execution.Process, anExecution *execution.Execution, err error) error {
	anExecution.Error += err.Error()
	return s.handleProcessedExecution(ctx, process, anExecution, execution.TaskStateFailed)
}
