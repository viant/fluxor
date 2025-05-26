package approval

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/viant/fluxor/model/execution"
	"github.com/viant/fluxor/policy"
	"github.com/viant/fluxor/service/dao"
	"github.com/viant/fluxor/service/messaging"
	"github.com/viant/fluxor/tracing"
)

// Service consumes approval requests, calls an AskFunc and records the
// decision by updating the corresponding execution in the DAO.  If approved,
// the execution is re-published to the main work queue so that a worker can
// run it.
//
// The service is intended to run either embedded (developer mode) or as a
// standalone micro-service.  It is safe for concurrent use and supports
// graceful shutdown via Shutdown().
type Service struct {
	reqQueue  messaging.Queue[Request]
	workQueue messaging.Queue[execution.Execution]

	execDAO dao.Service[string, execution.Execution]

	askFn policy.AskFunc

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// NewService constructs the approval service.  ask must not be nil.
func NewService(reqQ messaging.Queue[Request], workQ messaging.Queue[execution.Execution], dao dao.Service[string, execution.Execution], ask policy.AskFunc) *Service {
	return &Service{
		reqQueue:  reqQ,
		workQueue: workQ,
		execDAO:   dao,
		askFn:     ask,
	}
}

// Start begins consuming approval requests.  It will block until the supplied
// context is cancelled or an unrecoverable queue error occurs.
func (s *Service) Start(ctx context.Context) error {
	if s.askFn == nil {
		return fmt.Errorf("approval: AskFunc must not be nil")
	}

	ctx, s.cancel = context.WithCancel(ctx)

	for {
		select {
		case <-ctx.Done():
			s.wg.Wait()
			return ctx.Err()
		default:
		}

		msg, err := s.reqQueue.Consume(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				s.wg.Wait()
				return err
			}
			return err
		}
		if msg == nil {
			continue
		}

		// Handle each request concurrently so slow user interaction does not
		// block the consumer loop.
		s.wg.Add(1)
		go func(m messaging.Message[Request]) {
			defer s.wg.Done()
			s.handle(ctx, m)
		}(msg)
	}
}

// Shutdown signals the consumer loop to stop and waits until all in-flight
// requests complete.
func (s *Service) Shutdown() {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
}

// ---------------------------------------------------------------------------
// internals
// ---------------------------------------------------------------------------

func (s *Service) handle(ctx context.Context, msg messaging.Message[Request]) {
	req := msg.T()

	// Start tracing span for user prompt.
	ctx, span := tracing.StartSpan(ctx, fmt.Sprintf("approval.ask %s", req.Action), "SERVER")
	span.WithAttributes(map[string]string{"execution.id": req.ExecutionID})

	approved := s.askFn(ctx, req.Action, req.Args, nil)

	// End span but defer so we capture DAO errors.
	defer tracing.EndSpan(span, nil)

	exec, err := s.execDAO.Load(ctx, req.ExecutionID)
	if err != nil {
		_ = msg.Ack()
		log.Printf("approval: failed to load execution %s: %v", req.ExecutionID, err)
		return
	}
	if exec == nil {
		_ = msg.Ack()
		return
	}

	// If already decided, idempotently acknowledge.
	if exec.Approved != nil {
		_ = msg.Ack()
		return
	}

	exec.Approved = &approved
	if approved {
		exec.State = execution.TaskStatePending
	} else {
		exec.State = execution.TaskStateFailed
	}

	if err = s.execDAO.Save(ctx, exec); err != nil {
		log.Printf("approval: failed to save execution %s: %v", exec.ID, err)
		_ = msg.Nack(err)
		return
	}

	// If approved push back to work queue so a worker executes the action.
	if approved && s.workQueue != nil {
		_ = s.workQueue.Publish(ctx, exec)
	}

	_ = msg.Ack()
}
