package approval

import (
	"context"
	"time"
)

// DecisionFunc decides what to do with a pending request.
// Return (true,  "") to approve
//
//	(false, "…") to reject with reason.
type DecisionFunc func(r *Request) (approved bool, reason string)

// AutoDecider starts a goroutine that polls ListPending and applies fn to
// every request.  It returns stop() – call it (or cancel ctx) to exit.
func AutoDecider(ctx context.Context,
	svc Service,
	fn DecisionFunc,
	interval time.Duration) (stop func()) {

	if interval <= 0 {
		interval = 20 * time.Millisecond
	}
	done := make(chan struct{})

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-ticker.C:
				reqs, _ := svc.ListPending(ctx)
				for _, r := range reqs {
					ok, reason := fn(r)
					_, _ = svc.Decide(ctx, r.ID, ok, reason)
				}
			}
		}
	}()
	return func() { close(done) }
}

// AutoApprove automatically approves all pending requests
func AutoApprove(ctx context.Context,
	svc Service,
	interval time.Duration) func() {
	return AutoDecider(ctx, svc,
		func(*Request) (bool, string) {
			return true, ""
		}, interval)
}

// AutoReject automatically rejects all pending requests with the given reason
func AutoReject(ctx context.Context,
	svc Service,
	reason string,
	interval time.Duration) func() {
	return AutoDecider(ctx, svc,
		func(*Request) (bool, string) { return false, reason }, interval)
}

// AutoExpire periodically checks pending requests and auto-rejects those whose
// ExpiresAt deadline has passed.  It publishes a request.expired event before
// recording the rejection decision.
//
//	stop := approval.AutoExpire(ctx, svc, "expired", 1*time.Second)
//
// Passing interval <= 0 defaults to 30 s.  Empty reason defaults to "expired".
func AutoExpire(ctx context.Context,
	svc Service,
	reason string,
	interval time.Duration) (stop func()) {

	if interval <= 0 {
		interval = 30 * time.Second
	}
	if reason == "" {
		reason = "expired"
	}

	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		q := svc.Queue()

		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-ticker.C:
				now := time.Now()
				pending, _ := svc.ListPending(ctx)
				for _, r := range pending {
					if r == nil || r.ExpiresAt == nil {
						continue
					}
					if r.ExpiresAt.Before(now) {
						// Inform listeners the request has expired prior to decision.
						if q != nil {
							_ = q.Publish(ctx, &Event{Topic: TopicRequestExpired, Data: r})
						}

						// Auto-reject with specified reason.
						_, _ = svc.Decide(ctx, r.ID, false, reason)
					}
				}
			}
		}
	}()

	return func() { close(done) }
}

/* --------------------------------------------------------------------------
Helper API – quality-of-life utilities requested by users
----------------------------------------------------------------------------*/

// PendingFilter is a predicate applied to *Request when filtering the list of
// pending approval requests.  All supplied filters must return true for a
// request to be included in the output slice.
type PendingFilter func(*Request) bool

// WithProcessID returns a PendingFilter that keeps requests whose ProcessID
// matches the supplied id (empty id disables the filter and passes everything).
func WithProcessID(id string) PendingFilter {
	if id == "" {
		return func(*Request) bool { return true }
	}
	return func(r *Request) bool { return r != nil && r.ProcessID == id }
}

// WithAction returns a PendingFilter that keeps requests whose Action matches
// the supplied value (empty string disables the filter).
func WithAction(action string) PendingFilter {
	if action == "" {
		return func(*Request) bool { return true }
	}
	return func(r *Request) bool { return r != nil && r.Action == action }
}

// ListPending returns pending requests that satisfy all provided filters.
// It delegates the retrieval to Service.ListPending and then applies the
// filters client-side, avoiding changes to the underlying Service interface.
//
// Example:
//
//	reqs, _ := approval.ListPending(ctx, svc, approval.WithProcessID(pid))
func ListPending(ctx context.Context, svc Service, filters ...PendingFilter) ([]*Request, error) {
	if svc == nil {
		return nil, nil
	}

	reqs, err := svc.ListPending(ctx)
	if err != nil || len(filters) == 0 {
		return reqs, err
	}

	out := make([]*Request, 0, len(reqs))
	next := func(r *Request) bool {
		for _, f := range filters {
			if !f(r) {
				return false
			}
		}
		return true
	}

	for _, r := range reqs {
		if next(r) {
			out = append(out, r)
		}
	}
	return out, nil
}

// WaitForDecision blocks until an approval decision for the given request ID
// appears on the Service event queue, or the context / timeout expires.
// It consumes messages from the queue and acknowledges them once processed.
// If timeout <= 0 the call relies solely on the lifetime of ctx.
func WaitForDecision(ctx context.Context, svc Service, id string, timeout time.Duration) (*Decision, error) {
	if id == "" || svc == nil {
		return nil, context.Canceled // treat as cancelled to keep API simple
	}

	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	q := svc.Queue()
	for {
		msg, err := q.Consume(ctx)
		if err != nil {
			return nil, err
		}

		evt := msg.T()
		// We are only interested in decision events (legacy or new name).
		if evt != nil && (evt.Topic == TopicDecisionCreated || evt.Topic == LegacyTopicDecisionNew) {
			if dec, ok := evt.Data.(*Decision); ok && dec != nil && dec.ID == id {
				_ = msg.Ack()
				return dec, nil
			}
		}

		_ = msg.Ack()
	}
}
