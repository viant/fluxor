package processor

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/viant/fluxor/model"
	"github.com/viant/fluxor/model/graph"
	"github.com/viant/fluxor/policy"
	"github.com/viant/fluxor/progress"
	execution "github.com/viant/fluxor/runtime/execution"
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
	processDAO       dao.Service[string, execution.Process]
	taskExecutionDao dao.Service[string, execution.Execution]
	sessListeners    []execution.StateListener
	whenListeners    []execution.WhenListener

	queue    messaging.Queue[execution.Execution]
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

func (s *Service) StartProcess(ctx context.Context, workflow *model.Workflow, init map[string]interface{}, customTasks ...string) (aProcess *execution.Process, err error) {
	// Ensure we have a progress tracker in the context. Root workflow creates
	// a new tracker; sub-workflows inherit the parent's tracker automatically.
	var tracker *progress.Progress
	if t, ok := progress.FromContext(ctx); ok {
		tracker = t
	} else {
		ctx, tracker = progress.WithNewTracker(ctx, "", workflow.Name, nil)
	}
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
	if tracker != nil && tracker.RootProcessID == "" {
		tracker.Lock()
		if tracker.RootProcessID == "" {
			tracker.RootProcessID = processID
		}
		tracker.Unlock()
	}
	span.WithAttributes(map[string]string{"process.id": processID})

	// Create the process
	aProcess = execution.NewProcess(processID, workflow.Name, workflow, init)
	aProcess.Ctx = ctx
	aProcess.Session.RegisterListeners(s.sessListeners...)
	aProcess.Session.RegisterWhenListeners(s.whenListeners...)

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
	if parentProc := execution.ContextValue[*execution.Process](ctx); parentProc != nil {
		aProcess.ParentID = parentProc.ID
	}

	// Apply initial state from workflow
	if workflow.Init != nil {
		aProcess.Session.ApplyParameters(workflow.Init)
	}
	anExecution := execution.NewExecution(processID, nil, workflow.Pipeline)
	aProcess.Push(anExecution)
	// Record initial task allocation in progress tracker.
	progress.UpdateCtx(ctx, progress.Delta{Total: 1, Pending: 1})

	// Set aProcess state to running
	aProcess.SetState(execution.StateRunning)

	if err = s.processDAO.Save(ctx, aProcess); err != nil {
		err = fmt.Errorf("failed to save process: %w", err)
		return
	}
	// No need to schedule tasks here - allocator will pick up
	return aProcess, nil
}

// GetProcess retrieves a process by ID
func (s *Service) GetProcess(ctx context.Context, processID string) (*execution.Process, error) {
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

	if process.GetState() != execution.StateRunning {
		return fmt.Errorf("process %s is not in running state", processID)
	}

	// Update the process state to paused
	process.SetState(execution.StatePaused)
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

	if process.GetState() != execution.StatePaused {
		return fmt.Errorf("process %s is not in paused state", processID)
	}

	// Update the process state to running
	process.SetState(execution.StateRunning)
	return s.processDAO.Save(ctx, process)
	// Let the allocator schedule next tasks
}

// ResumeFailedProcess resets a failed or cancelled workflow so that execution
// can continue.  It rewinds every execution that ended with failed/cancelled
// state back to pending and switches the parent process to running.  The
// allocator will then pick up the work in its next iteration.
func (s *Service) ResumeFailedProcess(ctx context.Context, processID string) error {
	proc, err := s.processDAO.Load(ctx, processID)
	if err != nil {
		return fmt.Errorf("failed to load process: %w", err)
	}
	if proc == nil {
		return fmt.Errorf("process %s not found", processID)
	}

	state := proc.GetState()
	if state != execution.StateFailed && state != execution.StateCancelled {
		return fmt.Errorf("process %s is not in failed/cancelled state", processID)
	}

	// Remove recorded errors and rewind executions.
	failedCount := 0
	proc.Errors = map[string]string{}
	for _, ex := range proc.Stack {
		switch ex.State {
		case execution.TaskStateFailed, execution.TaskStateCancelled:
			failedCount++
			ex.State = execution.TaskStatePending
			ex.Error = ""
		}
	}

	// Reactivate
	proc.SetState(execution.StateRunning)

	// Update progress tracker if available
	if failedCount > 0 {
		progress.UpdateCtx(proc.Ctx, progress.Delta{Failed: -failedCount, Pending: +failedCount})
	}

	return s.processDAO.Save(ctx, proc)
}

// CancelProcess requests cancellation of a running or paused process. It marks
// the process as cancelRequested and propagates context cancellation so that
// any in-flight task can terminate early.  The allocator will move the process
// to the final "cancelled" state once the current stack is drained.
func (s *Service) CancelProcess(ctx context.Context, processID string, reason string) error {
	process, err := s.processDAO.Load(ctx, processID)
	if err != nil {
		return fmt.Errorf("failed to load process: %w", err)
	}
	if process == nil {
		return fmt.Errorf("process %s not found", processID)
	}

	state := process.GetState()
	if state == execution.StateCancelled {
		return nil // already cancelled
	}

	// Set cancel flag and cancel context to interrupt running tasks.
	process.CancelCtx()
	process.CancelReason = reason
	process.SetState(execution.StateCancelRequested)
	return s.processDAO.Save(ctx, process)
}

// processMessage handles a single task execution message
func (s *Service) processMessage(ctx context.Context, message messaging.Message[execution.Execution]) (err error) {

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
	if process.GetState() == execution.StatePaused {
		// Don't mark as failed, just requeue with a delay
		return message.Nack(fmt.Errorf("process is paused"))
	}
	// If cancellation has been requested, do not continue executing this task –
	// acknowledge so that allocator can eventually mark the process as
	// cancelled once the queue drains.
	if process.GetState() == execution.StateCancelRequested {
		return message.Ack()
	}

	// Transition counters: pending -> running
	progress.UpdateCtx(process.Ctx, progress.Delta{Pending: -1, Running: +1})
	// If cancellation has been requested, do not continue executing this task –
	// acknowledge so that allocator can eventually mark the process as
	// cancelled once the queue drains.
	if process.GetState() == execution.StateCancelRequested {
		return message.Ack()
	}

	// Derive execution context from the per-process context so that cancellation
	// requests propagate to running tasks. Fall back to incoming ctx if the
	// stored context is nil (e.g. legacy processes loaded from store).
	baseCtx := process.Ctx
	if baseCtx == nil {
		baseCtx = ctx
	}
	execCtx := context.WithValue(baseCtx, execution.ProcessKey, process)
	execCtx = context.WithValue(execCtx, execution.ExecutionKey, anExecution)

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
			anExecution.State = execution.TaskStateScheduled
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
						proc.SetDep(parent, inProc.TaskID, execution.TaskStateFailed)
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
		progress.UpdateCtx(process.Ctx, progress.Delta{Running: -1, Failed: +1})
		progress.UpdateCtx(process.Ctx, progress.Delta{Running: -1, Failed: +1})
		if daoErr := s.taskExecutionDao.Save(ctx, anExecution); daoErr != nil {
			return message.Nack(fmt.Errorf("encounter error: %w, and failed to save execution: %v", err, daoErr))
		}

		// ------------------------------------------------------------------
		// Propagate the failed state to the process so that the allocator can
		// advance the workflow and eventually mark the entire run as failed.
		// ------------------------------------------------------------------
		if proc, pErr := s.processDAO.Load(ctx, anExecution.ProcessID); pErr == nil && proc != nil {
			if inProc := proc.LookupExecution(anExecution.TaskID); inProc != nil {
				inProc.State = execution.TaskStateFailed
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
			proc.SetState(execution.StateFailed)
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
		// running -> completed
		progress.UpdateCtx(process.Ctx, progress.Delta{Running: -1, Completed: +1})
	}

	// Update the process with the completed execution
	if err := s.taskExecutionDao.Save(ctx, anExecution); err != nil {
		return message.Nack(err)
	}

	// allocator will pick up the persisted execution and update the process;
	// processor is intentionally read-only from this point on.
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

func (s *Service) Listeners() []execution.StateListener {
	return s.sessListeners
}
