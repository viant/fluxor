package memory

import (
	"context"
	"errors"
	"fmt"
	"time"

	execution "github.com/viant/fluxor/runtime/execution"
	approval "github.com/viant/fluxor/service/approval"
	"github.com/viant/fluxor/service/dao"
	"github.com/viant/fluxor/service/dao/store"
	"github.com/viant/fluxor/service/messaging"
	qmem "github.com/viant/fluxor/service/messaging/memory"
)

type service struct {
	executionDao dao.Service[string, execution.Execution]

	// DAO-backed stores
	reqDAO dao.Service[string, approval.Request]
	decDAO dao.Service[string, approval.Decision]

	// fan-out queue
	events messaging.Queue[approval.Event]

	// owning process store (optional – only needed when we want to update the
	// execution embedded in the process' stack after an approval decision).
	processDao dao.Service[string, execution.Process]
}

// key selectors – grab ID field
func reqKey(r *approval.Request) string  { return r.ID }
func decKey(d *approval.Decision) string { return d.ID }

func New(executionDao dao.Service[string, execution.Execution], options ...Option) approval.Service {
	ret := &service{
		executionDao: executionDao,
		reqDAO:       store.NewMemoryStore[string, approval.Request](reqKey),
		decDAO:       store.NewMemoryStore[string, approval.Decision](decKey),
		events:       qmem.NewQueue[approval.Event](qmem.DefaultConfig()),
	}
	for _, option := range options {
		option(ret)
	}
	return ret
}

/* ---------------- DAO-style operations -------------------------------- */

func (s *service) RequestApproval(ctx context.Context, r *approval.Request) error {
	if r == nil {
		return errors.New("invalid request")
	}

	// Ensure the request has a globally unique identifier.  If the caller did
	// not specify one we fallback to ExecutionID (which is guaranteed to be
	// unique within a run) – this keeps the function generic and protects
	// against silent drops caused by empty IDs.
	if r.ID == "" {
		switch {
		case r.ExecutionID != "":
			r.ID = r.ExecutionID
		case r.ProcessID != "":
			r.ID = fmt.Sprintf("%s/%d", r.ProcessID, time.Now().UnixNano())
		default:
			r.ID = fmt.Sprintf("anon-%d", time.Now().UnixNano())
		}
	}

	// Idempotent save – overwrite any previous copy to handle re-submissions
	// gracefully.
	_ = s.reqDAO.Save(ctx, r)
	_ = s.events.Publish(ctx, &approval.Event{Topic: approval.TopicRequestCreated, Data: r})
	_ = s.events.Publish(ctx, &approval.Event{Topic: approval.LegacyTopicRequestNew, Data: r})
	return nil
}

func (s *service) ListPending(ctx context.Context) ([]*approval.Request, error) {
	all, err := s.reqDAO.List(ctx)
	if err != nil {
		return nil, err
	}
	pending := make([]*approval.Request, 0, len(all))
	for _, r := range all {
		if d, _ := s.decDAO.Load(ctx, r.ID); d == nil {
			pending = append(pending, r)
		}
	}
	return pending, nil
}

func (s *service) Decide(ctx context.Context, id string,
	ok bool, reason string) (*approval.Decision, error) {

	if id == "" {
		return nil, errors.New("empty id")
	}
	request, _ := s.reqDAO.Load(ctx, id)
	if request == nil {
		return nil, fmt.Errorf("request %s not found", id)
	}
	if d, _ := s.decDAO.Load(ctx, id); d != nil {
		return nil, fmt.Errorf("already decided")
	}

	//--------------------------------------------------------------------------------
	// If the service has been initialised with an execution DAO and the request is
	// linked to a concrete execution (ExecutionID != ""), update the execution so
	// that the allocator can re-schedule or skip it accordingly.
	//
	// The DAO is optional because the approval service can be used as a standalone
	// component (e.g. to gate generic LLM tool invocations) where no execution
	// tracking exists.  In such cases we silently skip the execution update step.
	//--------------------------------------------------------------------------------

	if s.executionDao != nil && request.ExecutionID != "" {
		anExecution, err := s.executionDao.Load(ctx, request.ExecutionID)
		if err != nil {
			return nil, err
		}

		anExecution.Approved = &ok
		anExecution.ApprovalReason = reason
		if !ok {
			anExecution.Error = fmt.Sprintf("action %s rejected: %s", request.Action, reason)
		} else {
			anExecution.Error = ""
		}
		// Reset execution State so that allocator re-schedules it.
		anExecution.State = execution.TaskStatePending

		if err = s.executionDao.Save(ctx, anExecution); err != nil {
			return nil, err
		}

		// ------------------------------------------------------------------
		// Update the parent process copy so that allocator sees the change.
		// ------------------------------------------------------------------
		if s.processDao != nil {
			if proc, pErr := s.processDao.Load(ctx, request.ProcessID); pErr == nil && proc != nil {
				if ex := proc.LookupExecution(anExecution.TaskID); ex != nil {
					ex.Approved = anExecution.Approved
					ex.ApprovalReason = reason
					ex.State = execution.TaskStatePending
					_ = s.processDao.Save(ctx, proc)
				}
			}
		}
	}

	d := &approval.Decision{
		ID:        id,
		Approved:  ok,
		Reason:    reason,
		DecidedAt: time.Now(),
	}
	_ = s.decDAO.Save(ctx, d)
	_ = s.events.Publish(ctx, &approval.Event{Topic: approval.TopicDecisionCreated, Data: d})
	_ = s.events.Publish(ctx, &approval.Event{Topic: approval.LegacyTopicDecisionNew, Data: d})
	return d, nil
}

/* ---------------- Broker-style ---------------------------------------- */

func (s *service) Queue() messaging.Queue[approval.Event] { return s.events }

var _ approval.Service = (*service)(nil)
