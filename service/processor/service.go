package processor

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/viant/fluxor/model"
	"github.com/viant/fluxor/model/execution"
	"github.com/viant/fluxor/service/dao"
	"github.com/viant/fluxor/service/executor"
	"github.com/viant/fluxor/service/messaging"
	"github.com/viant/fluxor/tracing"
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
		MaxTaskRetries: 3,
		RetryDelay:     5 * time.Second,
	}
}

// Service handles workflow execution
type Service struct {
	config           Config
	processDAO       dao.Service[string, execution.Process]
	taskExecutionDao dao.Service[string, execution.Execution]

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
		select {
		case <-w.ctx.Done():
			return
		case <-w.service.shutdownCh:
			return
		default:
			// Subscribe to next message with timeout
			ctx, cancel := context.WithTimeout(w.ctx, 1*time.Second)
			message, err := w.service.queue.Consume(ctx)
			cancel()

			if err != nil {
				// Just a timeout, continue
				continue
			}

			if message != nil {
				go func() {
					err = w.service.processMessage(w.ctx, message)
					if err != nil {
						fmt.Printf("failed to process message: %v\n", err)
					}
				}()
			}
		}
	}
}

// StartProcess begins execution of a workflow
func (s *Service) StartProcess(ctx context.Context, workflow *model.Workflow, init map[string]interface{}, customTasks ...string) (aProcess *execution.Process, err error) {
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
	aProcess = execution.NewProcess(processID, workflow.Name, workflow, init)

	// Apply initial state from workflow
	if workflow.Init != nil {
		aProcess.Session.ApplyParameters(workflow.Init)
	}
	anExecution := execution.NewExecution(processID, nil, workflow.Pipeline)
	aProcess.Push(anExecution)

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

	err = s.executor.Execute(ctx, anExecution, process)
	if err != nil {
		anExecution.Fail(err)
		if daoErr := s.taskExecutionDao.Save(ctx, anExecution); daoErr != nil {
			return message.Nack(fmt.Errorf("encounter error: %w, and failed to save execution: %v", err, daoErr))
		}
		message.Ack()
		return nil
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
