// Package progress provides a lightweight tracker that keeps aggregated
// execution counters (tasks total, completed, failed, …) for a single
// workflow run.  The tracker instance lives in the execution context – every
// component that receives the context can atomically update the counters via
// the Delta helper without requiring a global registry.

package progress

import (
	"context"
	"sync"
	"time"
)

// Delta represents an incremental counter change emitted by the allocator,
// executor or processor.  The fields are signed and therefore can be either
// positive (increment) or negative (decrement).
type Delta struct {
	Total     int
	Completed int
	Skipped   int
	Failed    int
	Running   int
	Pending   int
}

// Progress keeps aggregated task counters for the root workflow and all its
// sub-workflows.  It is safe for concurrent use.
type Progress struct {
	// Identification – informative only, filled when the root workflow starts.
	RootProcessID string
	Workflow      string
	StartedAt     time.Time

	// Counters – modified via Update().
	TotalTasks     int
	CompletedTasks int
	SkippedTasks   int
	FailedTasks    int
	RunningTasks   int
	PendingTasks   int

	sync.Mutex
	onChange func(Progress)
}

// Update applies the supplied delta to the tracker.  It is safe to call from
// multiple goroutines.  If an onChange callback has been registered it will be
// invoked with a copy of the updated tracker outside the critical section so
// that the callback can perform slow operations (e.g. JSON encoding, I/O)
// without blocking engine internals.
func (p *Progress) Update(d Delta) {
	if p == nil {
		return
	}

	p.Lock()

	p.TotalTasks += d.Total
	p.CompletedTasks += d.Completed
	p.SkippedTasks += d.Skipped
	p.FailedTasks += d.Failed
	p.RunningTasks += d.Running
	p.PendingTasks += d.Pending

	// Make a value-copy for the callback while we still hold the lock to
	// avoid seeing partially updated counters.
	snapshot := *p
	cb := p.onChange

	p.Unlock()

	if cb != nil {
		cb(snapshot)
	}
}

// Snapshot returns a copy of the tracker suitable for read-only inspection.
func (p *Progress) Snapshot() Progress {
	if p == nil {
		return Progress{}
	}
	p.Lock()
	defer p.Unlock()
	return *p
}

// OnChange registers a callback that is invoked after every successful
// Update.  Passing nil disables the callback.  Only one callback can be
// active; subsequent calls overwrite the previous value.
func (p *Progress) OnChange(cb func(Progress)) {
	if p == nil {
		return
	}
	p.Lock()
	p.onChange = cb
	p.Unlock()
}

// ----------------------------------------------------------------------------
// Context helpers
// ----------------------------------------------------------------------------

type trackerKeyT struct{}

var trackerKey trackerKeyT

// WithNewTracker creates a new Progress tracker, embeds it in a derived
// context and returns both.  The caller may optionally pass an onChange
// callback that will be invoked after every counter update.
func WithNewTracker(ctx context.Context, rootProcessID, workflow string, onChange func(Progress)) (context.Context, *Progress) {
	if ctx == nil {
		ctx = context.Background()
	}
	tr := &Progress{
		RootProcessID: rootProcessID,
		Workflow:      workflow,
		StartedAt:     time.Now(),
		onChange:      onChange,
	}
	return context.WithValue(ctx, trackerKey, tr), tr
}

// FromContext extracts the Progress tracker from ctx.  It returns (tracker,
// ok).  The second return value is false when the context carries no tracker.
func FromContext(ctx context.Context) (*Progress, bool) {
	if ctx == nil {
		return nil, false
	}
	tr, ok := ctx.Value(trackerKey).(*Progress)
	return tr, ok
}

// GetSnapshot is a convenience wrapper that combines FromContext and
// Snapshot.  The boolean return value is false when the context does not
// carry a tracker.
func GetSnapshot(ctx context.Context) (Progress, bool) {
	if tr, ok := FromContext(ctx); ok {
		return tr.Snapshot(), true
	}
	return Progress{}, false
}

// UpdateCtx is a helper that looks up the tracker in ctx (if any) and applies
// the supplied delta.
func UpdateCtx(ctx context.Context, d Delta) {
	if tr, ok := FromContext(ctx); ok {
		tr.Update(d)
	}
}
