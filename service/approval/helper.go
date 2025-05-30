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
		func(*Request) (bool, string) { return true, "" }, interval)
}

// AutoReject automatically rejects all pending requests with the given reason
func AutoReject(ctx context.Context,
	svc Service,
	reason string,
	interval time.Duration) func() {
	return AutoDecider(ctx, svc,
		func(*Request) (bool, string) { return false, reason }, interval)
}
