package processor

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/viant/fluxor/model"
	"github.com/viant/fluxor/model/graph"
	"github.com/viant/fluxor/policy"
	execution2 "github.com/viant/fluxor/runtime/execution"
	"github.com/viant/fluxor/service/dao"
	"github.com/viant/fluxor/service/executor"
	"github.com/viant/fluxor/service/messaging"
	"github.com/viant/fluxor/tracing"
	"log"
	"math"
	"strings"
	"sync"
	"time"
)

// Config represents executor service configuration
type Config struct {
	// WorkerCount is the number of workers processing tasks
	WorkerCount int

	// MaxTaskRetries is the maximum number of retries for a task
	MaxTaskRetries int

	// RetryDelay is the delay between task retry attempts
	RetryDelay time.Duration
}

// DefaultConfig returns the default executor configuration
func DefaultConfig() Config {
	return Config{
		WorkerCount:    5,
		MaxTaskRetries: 1,
		RetryDelay:     3 * time.Second,
	}
}

// Service handles workflow execution
type Service struct {
	config           Config
	processDAO       dao.Service[string, execution2.Process]
	taskExecutionDao dao.Service[string, execution2.Execution]

	queue    messaging.Queue[execution2.Execution]
	executor executor.Service

	// Track active executions
	workers    []*worker
	workerWg   sync.WaitGroup
	shutdownCh chan struct{}
}

type worker struct {
	id       int
	service  *Service
	ctx      context.Context
	cancelFn context.CancelFunc
}

// ---------------------------------------------------------------------------
// Retry helpers
// ---------------------------------------------------------------------------

// shouldRetry returns (retry?, delay)
func (s *Service) shouldRetry(cfg *graph.Retry, attempts int) (bool, time.Duration) {
	// Use defaults when cfg nil
	if cfg == nil {
		if attempts >= s.config.MaxTaskRetries {
			return false, 0
		}
		return true, s.config.RetryDelay
	}

	if strings.ToLower(cfg.Type) == "none" {
		return false, 0
	}

	max := cfg.MaxRetries
	if max == 0 {
		max = s.config.MaxTaskRetries
	}
	if attempts >= max {
		return false, 0
	}

	// Parse base delay
	baseDelay := s.config.RetryDelay
	if cfg.Delay != "" {
		if d, err := time.ParseDuration(cfg.Delay); err == nil {
			baseDelay = d
		}
	}

	switch strings.ToLower(cfg.Type) {
	case "exponential":
		mult := cfg.Multiplier
		if mult <= 1 {
			mult = 2
		}
		delay := float64(baseDelay) * math.Pow(mult, float64(attempts))
		maxDelay := cfg.MaxDelay
		if maxDelay != "" {
			if md, err := time.ParseDuration(maxDelay); err == nil {
				if time.Duration(delay) > md {
					delay = float64(md)
				}
			}
		}
		return true, time.Duration(delay)
	default: // fixed
		return true, baseDelay
	}
}

// New creates a new executor service
func New(options ...Option) (*Service, error) {
	s := &Service{
		config:     DefaultConfig(),
		shutdownCh: make(chan struct{}),
	}
	for _, opt := range options {
		opt(s)
	}
	if s.executor == nil {
		return nil, fmt.Errorf("executor is required")
	}
	if s.queue == nil {
		return nil, fmt.Errorf("message queue is required")
	}
	if s.processDAO == nil {
		return nil, fmt.Errorf("processDAO service is required")
	}
	if s.taskExecutionDao == nil {
		return nil, fmt.Errorf("taskExecutionDao service is required")
	}

	return s, nil
}

// Start begins the task execution service
func (s *Service) Start(ctx context.Context) error {
	// Start worker goroutines
	for i := 0; i < s.config.WorkerCount; i++ {
		workerCtx, cancel := context.WithCancel(ctx)
		worker := &worker{
			id:       i,
			service:  s,
			ctx:      workerCtx,
			cancelFn: cancel,
		}
		s.workers = append(s.workers, worker)
		s.workerWg.Add(1)
		go worker.run()
	}

	return nil
}

// run processes messages from the queue
func (w *worker) run() {
	defer w.service.workerWg.Done()

	for {
		// Block until we either get a message or the context is cancelled.
		msg, err := w.service.queue.Consume(w.ctx)
		if err != nil {
			// Context was cancelled – graceful shutdown.
			if errors.Is(err, context.Canceled) {
				return
			}
			// Transient error (e.g. queue closed); back off a bit.
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if msg == nil {
			continue
		}

		// Process the message in-line; if you want maximum parallelism spawn a
		// goroutine here, but be mindful of ordering requirements.
		if pErr := w.service.processMessage(w.ctx, msg); pErr != nil {
			// Replace println with structured logging later.
			log.Printf("worker %d: failed to process message: %v", w.id, pErr)
		}
	}
}

// StartProcess begins execution of a workflow
func (s *Service) StartProcess(ctx context.Context, workflow *model.Workflow, init map[string]interface{}, customTasks ...string) (aProcess *execution2.Process, err error) {
	// start tracing span for process start
	ctx, span := tracing.StartSpan(ctx, fmt.Sprintf("processor.StartProcess %s", workflow.Name), "INTERNAL")
	defer tracing.EndSpan(span, err)
	span.WithAttributes(map[string]string{"workflow.name": workflow.Name})
	if workflow == nil {
		err = fmt.Errorf("workflow cannot be nil")
		return
	}

	// Generate a unique process ID
	processID := workflow.Name + "/" + uuid.New().String()
	span.WithAttributes(map[string]string{"process.id": processID})

	// Create the process
	aProcess = execution2.NewProcess(processID, workflow.Name, workflow, init)

	// Propagate policy (if any) from the incoming context so that executor can
	// enforce it later on.
	if p := policy.FromContext(ctx); p != nil {
		aProcess.Policy = policy.ToConfig(p)
	}

	// Start a parent tracing span covering the whole process lifetime
	ctx, procSpan := tracing.StartSpan(ctx, fmt.Sprintf("process.run %s", workflow.Name), "INTERNAL")
	procSpan.WithAttributes(map[string]string{"process.id": processID, "workflow.name": workflow.Name})
	aProcess.Span = procSpan

	// If the incoming context contains a running parent process, record its ID
	if parentProc := execution2.ContextValue[*execution2.Process](ctx); parentProc != nil {
		aProcess.ParentID = parentProc.ID
	}

	// Apply initial state from workflow
	if workflow.Init != nil {
		aProcess.Session.ApplyParameters(workflow.Init)
	}
	anExecution := execution2.NewExecution(processID, nil, workflow.Pipeline)
	aProcess.Push(anExecution)

	// Set aProcess state to running
	aProcess.SetState(execution2.StateRunning)

	if err = s.processDAO.Save(ctx, aProcess); err != nil {
		err = fmt.Errorf("failed to save process: %w", err)
		return
	}
	// No need to schedule tasks here - allocator will pick up
	return aProcess, nil
}

// GetProcess retrieves a process by ID
func (s *Service) GetProcess(ctx context.Context, processID string) (*execution2.Process, error) {
	return s.processDAO.Load(ctx, processID)
}

// PauseProcess pauses a running process
func (s *Service) PauseProcess(ctx context.Context, processID string) error {
	process, err := s.processDAO.Load(ctx, processID)
	if err != nil {
		return fmt.Errorf("failed to load process: %w", err)
	}
	if process == nil {
		return fmt.Errorf("process %s not found", processID)
	}

	if process.GetState() != execution2.StateRunning {
		return fmt.Errorf("process %s is not in running state", processID)
	}

	// Update the process state to paused
	process.SetState(execution2.StatePaused)
	return s.processDAO.Save(ctx, process)
}

// ResumeProcess resumes a paused process
func (s *Service) ResumeProcess(ctx context.Context, processID string) error {
	process, err := s.processDAO.Load(ctx, processID)
	if err != nil {
		return fmt.Errorf("failed to load process: %w", err)
	}
	if process == nil {
		return fmt.Errorf("process %s not found", processID)
	}

	if process.GetState() != execution2.StatePaused {
		return fmt.Errorf("process %s is not in paused state", processID)
	}

	// Update the process state to running
	process.SetState(execution2.StateRunning)
	return s.processDAO.Save(ctx, process)
	// Let the allocator schedule next tasks
}

// processMessage handles a single task execution message
func (s *Service) processMessage(ctx context.Context, message messaging.Message[execution2.Execution]) (err error) {

	anExecution := message.T()

	// Start the execution
	anExecution.Start()
	if err := s.taskExecutionDao.Save(ctx, anExecution); err != nil {
		return message.Nack(err)
	}

	// Get the process
	process, err := s.GetProcess(ctx, anExecution.ProcessID)
	if err != nil {
		return message.Nack(err)
	}

	// Check if process is paused - if so, requeue the message for later processing
	if process.GetState() == execution2.StatePaused {
		// Don't mark as failed, just requeue with a delay
		return message.Nack(fmt.Errorf("process is paused"))
	}

	// Ensure that the child execution receives information about the current process and execution
	execCtx := context.WithValue(ctx, execution2.ProcessKey, process)
	execCtx = context.WithValue(execCtx, execution2.ExecutionKey, anExecution)

	// Execute the task action.  Special-case ErrWaitForApproval which is a
	// transitional state rather than a real failure.

	err = s.executor.Execute(execCtx, anExecution, process)

	if err != nil {
		// ------------------------------------------------------------------
		// Retry handling (all other errors)
		// ------------------------------------------------------------------
		taskDef := process.LookupTask(anExecution.TaskID)
		retryCfg := taskDef.Retry
		shouldRetry, delay := s.shouldRetry(retryCfg, anExecution.Attempts)
		if shouldRetry {
			anExecution.Attempts++
			runAt := time.Now().Add(delay)
			anExecution.RunAfter = &runAt
			anExecution.State = execution2.TaskStateScheduled
			if daoErr := s.taskExecutionDao.Save(ctx, anExecution); daoErr != nil {
				return message.Nack(fmt.Errorf("error %w and failed to save execution: %v", err, daoErr))
			}

			// ------------------------------------------------------------------
			// Keep the execution embedded inside the parent process up to date so
			// that the allocator sees the correct RunAfter/Attempts values and does
			// not immediately reschedule the same task in a tight loop.
			// ------------------------------------------------------------------
			if proc, pErr := s.processDAO.Load(ctx, anExecution.ProcessID); pErr == nil && proc != nil {
				if inProc := proc.LookupExecution(anExecution.TaskID); inProc != nil {
					inProc.RunAfter = anExecution.RunAfter
					inProc.Attempts = anExecution.Attempts
					inProc.State = anExecution.State
					inProc.Error = err.Error()
					// propagate error to process-level map so that allocator can mark the
					// whole process as failed once the stack drains.
					if task := proc.LookupTask(anExecution.TaskID); task != nil {
						procKey := task.Namespace
						if procKey == "" {
							procKey = task.ID
						}
						proc.Errors[procKey] = err.Error()
					}
					// mark dependency in parent execution (if any)
					if parent := proc.LookupExecution(inProc.ParentTaskID); parent != nil {
						parent.Dependencies[inProc.TaskID] = execution2.TaskStateFailed
					}
					// remove failed execution from the stack so that allocator can
					// continue evaluating remaining tasks.
					proc.Remove(inProc)
				}
				_ = s.processDAO.Save(ctx, proc)
			}
			return message.Ack()
		}

		// Give up – mark as failed
		anExecution.Fail(err)
		if daoErr := s.taskExecutionDao.Save(ctx, anExecution); daoErr != nil {
			return message.Nack(fmt.Errorf("encounter error: %w, and failed to save execution: %v", err, daoErr))
		}

		// ------------------------------------------------------------------
		// Propagate the failed state to the process so that the allocator can
		// advance the workflow and eventually mark the entire run as failed.
		// ------------------------------------------------------------------
		if proc, pErr := s.processDAO.Load(ctx, anExecution.ProcessID); pErr == nil && proc != nil {
			if inProc := proc.LookupExecution(anExecution.TaskID); inProc != nil {
				inProc.State = execution2.TaskStateFailed
				inProc.Error = anExecution.Error
			}

			// Record error under namespace OR fallback to TaskID so the user can
			// see which task failed.
			if task := proc.LookupTask(anExecution.TaskID); task != nil {
				key := task.Namespace
				if key == "" {
					key = task.ID
				}
				proc.Errors[key] = err.Error()
			}

			// Stop the workflow immediately – clear remaining work and mark the
			// process as failed.
			proc.Stack = nil
			proc.SetState(execution2.StateFailed)
			_ = s.processDAO.Save(ctx, proc)
		}

		message.Ack()
		return nil
	}

	if anExecution.State.IsWaitForApproval() {
		// Update the process with the completed execution
		if err := s.taskExecutionDao.Save(ctx, anExecution); err != nil {
			return message.Nack(err)
		}
		return message.Ack()
	}

	task := process.LookupTask(anExecution.TaskID)
	if task.IsAutoPause() {
		anExecution.Pause()
	} else {
		anExecution.Complete()
	}

	// Update the process with the completed execution
	if err := s.taskExecutionDao.Save(ctx, anExecution); err != nil {
		return message.Nack(err)
	}
	return message.Ack()
}

// Shutdown stops the executor service
func (s *Service) Shutdown() {
	close(s.shutdownCh)
	for _, worker := range s.workers {
		worker.cancelFn()
	}
	s.workerWg.Wait()
}
