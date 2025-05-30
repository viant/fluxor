package approval

import (
	"context"
	"github.com/viant/fluxor/service/messaging"
)

// Service defines the approval service interface.
type Service interface {
	RequestApproval(ctx context.Context, r *Request) error
	ListPending(ctx context.Context) ([]*Request, error)
	Decide(ctx context.Context, id string, approved bool, reason string) (*Decision, error)
	Queue() messaging.Queue[Event]
}
