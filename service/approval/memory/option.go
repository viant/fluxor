package memory

import (
	execution "github.com/viant/fluxor/runtime/execution"
	"github.com/viant/fluxor/service/dao"
	"github.com/viant/fluxor/service/messaging"
)

type Option func(*service)

// WithProcessDAO allows the approval service to update the parent process when
// a decision is made.  The allocator then notices the changed execution state
// and schedules it for execution.
func WithProcessDAO(dao dao.Service[string, execution.Process]) Option {
	return func(s *service) { s.processDao = dao }
}

// WithExecutionQueue attaches the execution queue so that the approval service
// can re-schedule a task automatically after it has been approved.  This is
// required for ad-hoc executions that are not managed by the allocator – once
// a positive decision is recorded the service publishes the execution back to
// the queue so that the processor can pick it up.
func WithExecutionQueue(q messaging.Queue[execution.Execution]) Option {
	return func(s *service) { s.execQueue = q }
}
