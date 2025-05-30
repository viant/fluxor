package memory

import (
	execution "github.com/viant/fluxor/runtime/execution"
	"github.com/viant/fluxor/service/dao"
)

type Option func(*service)

// WithProcessDAO allows the approval service to update the parent process when
// a decision is made.  The allocator then notices the changed execution state
// and schedules it for execution.
func WithProcessDAO(dao dao.Service[string, execution.Process]) Option {
	return func(s *service) { s.processDao = dao }
}
