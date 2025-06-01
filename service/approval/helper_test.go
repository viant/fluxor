package approval_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"sort"

	approval "github.com/viant/fluxor/service/approval"
	memApproval "github.com/viant/fluxor/service/approval/memory"

	execution "github.com/viant/fluxor/runtime/execution"
	"github.com/viant/fluxor/service/dao"
)

// TestWaitForDecision verifies that WaitForDecision blocks until a decision is
// published on the service queue and returns the correct decision data.
func TestWaitForDecision(t *testing.T) {
	type testCase struct {
		name        string
		approve     bool
		expectError bool
		timeout     time.Duration
		decideDelay time.Duration
	}

	tests := []testCase{{
		name:        "approved before timeout",
		approve:     true,
		expectError: false,
		timeout:     500 * time.Millisecond,
		decideDelay: 10 * time.Millisecond,
	}, {
		name:        "rejected before timeout",
		approve:     false,
		expectError: false,
		timeout:     500 * time.Millisecond,
		decideDelay: 10 * time.Millisecond,
	}, {
		name:        "timeout waiting for decision",
		approve:     true, // irrelevant – decision never sent
		expectError: true,
		timeout:     50 * time.Millisecond,
		decideDelay: 100 * time.Millisecond, // triggered after timeout
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			// Execution DAO not required in these tests; provide typed nil.
			var execDAO dao.Service[string, execution.Execution]
			svc := memApproval.New(execDAO)

			reqID := "req-1"
			req := &approval.Request{
				ID:        reqID,
				ProcessID: "p1",
				Action:    "act1",
				CreatedAt: time.Now(),
			}

			// Register pending request.
			_ = svc.RequestApproval(ctx, req)

			// Schedule decision publication according to test case parameters.
			if tc.decideDelay > 0 {
				go func() {
					time.Sleep(tc.decideDelay)
					_, _ = svc.Decide(ctx, reqID, tc.approve, "")
				}()
			}

			dec, err := approval.WaitForDecision(ctx, svc, reqID, tc.timeout)

			if tc.expectError {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			expected := &approval.Decision{
				ID:       reqID,
				Approved: tc.approve,
			}
			if dec != nil {
				expected.DecidedAt = dec.DecidedAt // align dynamic field
			}
			assert.EqualValues(t, expected, dec)
		})
	}
}

// TestListPending verifies that ListPending helper applies filters correctly.
func TestListPending(t *testing.T) {
	ctx := context.Background()

	var execDAO dao.Service[string, execution.Execution]
	svc := memApproval.New(execDAO)

	now := time.Now()
	requests := []*approval.Request{
		{ID: "r1", ProcessID: "p1", Action: "a1", CreatedAt: now},
		{ID: "r2", ProcessID: "p1", Action: "a2", CreatedAt: now},
		{ID: "r3", ProcessID: "p2", Action: "a1", CreatedAt: now},
	}

	for _, r := range requests {
		_ = svc.RequestApproval(ctx, r)
	}

	type testCase struct {
		name     string
		filters  []approval.PendingFilter
		expected []*approval.Request
	}

	tests := []testCase{
		{
			name:     "filter by processID",
			filters:  []approval.PendingFilter{approval.WithProcessID("p1")},
			expected: []*approval.Request{requests[0], requests[1]},
		},
		{
			name:     "filter by action",
			filters:  []approval.PendingFilter{approval.WithAction("a1")},
			expected: []*approval.Request{requests[0], requests[2]},
		},
		{
			name:     "filter by processID and action",
			filters:  []approval.PendingFilter{approval.WithProcessID("p1"), approval.WithAction("a1")},
			expected: []*approval.Request{requests[0]},
		},
		{
			name:     "no filters",
			filters:  nil,
			expected: requests,
		},
	}

	// Helper to sort slice by ID to achieve deterministic comparison.
	sortByID := func(in []*approval.Request) []*approval.Request {
		out := make([]*approval.Request, len(in))
		copy(out, in)
		sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
		return out
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := approval.ListPending(ctx, svc, tc.filters...)
			assert.NoError(t, err)
			assert.EqualValues(t, sortByID(tc.expected), sortByID(actual))
		})
	}

	// -------- AutoExpire integration test --------
	t.Run("auto_expire_rejects", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var execDAO dao.Service[string, execution.Execution]
		svc := memApproval.New(execDAO)

		expireAt := time.Now().Add(-1 * time.Minute) // already expired
		req := &approval.Request{ID: "exp1", ProcessID: "pX", Action: "act", CreatedAt: time.Now(), ExpiresAt: &expireAt}
		_ = svc.RequestApproval(ctx, req)

		stop := approval.AutoExpire(ctx, svc, "expired", 10*time.Millisecond)
		defer stop()

		dec, err := approval.WaitForDecision(ctx, svc, req.ID, 500*time.Millisecond)
		assert.NoError(t, err)
		assert.EqualValues(t, &approval.Decision{ID: req.ID, Approved: false, Reason: "expired", DecidedAt: dec.DecidedAt}, dec)
	})
}
