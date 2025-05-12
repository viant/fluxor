package allocator

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/fluxor/model/execution"
	"github.com/viant/fluxor/model/expander"
	"github.com/viant/fluxor/model/graph"
	"github.com/viant/fluxor/service/dao"
	"github.com/viant/fluxor/service/event"
	"github.com/viant/fluxor/service/messaging"
	"log"
	"time"
)

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
	processes, err := s.processDAO.List(ctx, dao.NewParameter("State", execution.StatePending, execution.StateRunning))
	if err != nil {
		return fmt.Errorf("failed to list processes: %w", err)
	}

	// Process each running workflow
	for _, process := range processes {
		// Skip processes that aren't running
		if process.GetState() != execution.StateRunning {
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
	// Skip if process has reached max concurrent tasks

	// Check if there are tasks on the stack to execute
	if len(process.Stack) == 0 {
		// No more tasks to execute, check if process is complete
		if process.GetState() == execution.StateRunning {
			if len(process.Errors) > 0 {
				process.SetState(execution.StateFailed)
			} else {
				process.SetState(execution.StateCompleted)
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
	anExecution.Data = make(map[string]interface{})
	for _, parameter := range currentTask.Init {
		if anExecution.Data[parameter.Name], err = process.Session.Expand(parameter.Value); err != nil {
			return true, err
		}
	}
	return false, err
}

func (s *Service) handleRunningTask(ctx context.Context, process *execution.Process, anExecution *execution.Execution) (bool, error) {
	runningExecution, err := s.taskExecutionDao.Load(ctx, anExecution.ID)
	if err != nil {
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

	var scheduled []*execution.Execution
	// Check if all dependencies are satisfied
outer:
	for _, depID := range anExecution.DependsOn {
		task := process.LookupTask(depID)
		status := anExecution.Dependencies[task.ID]
		switch status {
		case execution.TaskStatePending:
			scheduled = append(scheduled, execution.NewExecution(process.ID, currentTask, task))
			anExecution.Dependencies[task.ID] = execution.TaskStateScheduled
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

	var scheduled []*execution.Execution
	// Check if all dependencies are satisfied
outer:
	for i := range currentTask.Tasks {
		task := currentTask.Tasks[i]
		status := anExecution.Dependencies[task.ID]
		switch status {
		case execution.TaskStatePending:
			scheduled = append(scheduled, execution.NewExecution(process.ID, currentTask, task))
			anExecution.Dependencies[task.ID] = execution.TaskStateScheduled
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
	status := anExecution.Dependencies[task.ID]
	if status == execution.TaskStatePending {
		depExecution := execution.NewExecution(process.ID, parent, task)
		anExecution.Dependencies[task.ID] = execution.TaskStateScheduled
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
		parentExecution.Dependencies[taskID] = execution.TaskStateCompleted
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

func (s *Service) handleProcessedExecution(ctx context.Context, process *execution.Process, anExecution *execution.Execution, state execution.TaskState) error {
	currentTask := process.LookupTask(anExecution.TaskID)

	if state == execution.TaskStateCompleted {
		output := anExecution.Output
		var outputMap = make(map[string]string)
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

func (s *Service) handleTaskDone(currentTask *graph.Task, process *execution.Process, anExecution *execution.Execution, outputMap map[string]string) error {

	source := process.Session.Clone()
	for k, v := range outputMap {
		source.Set(k, v)
	}

	for _, parameter := range currentTask.Post {
		evaluated, err := expander.Expand(parameter.Value, source.State)
		if err == nil {
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

func (s *Service) handleExecutionError(ctx context.Context, process *execution.Process, anExecution *execution.Execution, err error) error {
	anExecution.Error += err.Error()
	return s.handleProcessedExecution(ctx, process, anExecution, execution.TaskStateFailed)
}
