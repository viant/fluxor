package workflow

import (
	"context"
	"github.com/viant/fluxor/runtime/execution"
	"testing"

	"github.com/stretchr/testify/assert"

	processdao "github.com/viant/fluxor/service/dao/process/memory"
)

// TestWaitForProcess verifies that WaitForProcess returns as soon as the
// process reaches a terminal state and never blocks indefinitely.
func TestWaitForProcess(t *testing.T) {
	ctx := context.Background()

	// Prepare a completed process in the in-memory DAO.
	p := execution.NewProcess("proc-test", "demo", nil, nil)
	p.SetState(execution.StateCompleted)

	pDao := processdao.New()
	_ = pDao.Save(ctx, p)

	svc := New(nil, nil, pDao)

	// Use a generous timeout – the call should return immediately, not after
	// the entire duration.
	out, err := svc.WaitForProcess(ctx, p.ID, 1_000 /* 1 second */)

	assert.NoError(t, err)
	assert.EqualValues(t, execution.StateCompleted, out.State)
	assert.False(t, out.Timeout)
}
