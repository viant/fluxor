package fluxor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/fluxor/model/graph"
	execution "github.com/viant/fluxor/runtime/execution"
)

// TestRuntime_QueueExecution verifies that QueueExecution publishes an execution
// to the processor queue, and that it can be consumed directly.
func TestRuntime_QueueExecution(t *testing.T) {
	svc := New()
	ctx := svc.NewContext(context.Background())
	rt := svc.Runtime()
	// create a dummy task for the execution
	task := &graph.Task{ID: "dummy"}
	exec := execution.NewExecution("proc1", nil, task)
	require.NoError(t, rt.QueueExecution(ctx, exec))
	// the message should now be available on the shared queue
	msg, err := svc.queue.Consume(ctx)
	require.NoError(t, err)
	require.Equal(t, exec.ID, msg.T().ID)
}

// TestRuntime_WaitForExecutionStates verifies WaitForExecution returns once
// the execution enters a terminal or paused/approval state.
func TestRuntime_WaitForExecutionStates(t *testing.T) {
	svc := New()
	ctx := svc.NewContext(context.Background())
	rt := svc.Runtime()

	testCases := []struct {
		name  string
		state execution.TaskState
	}{
		{"completed", execution.TaskStateCompleted},
		{"failed", execution.TaskStateFailed},
		{"skipped", execution.TaskStateSkipped},
		{"cancelled", execution.TaskStateCancelled},
		{"paused", execution.TaskStatePaused},
		{"waitForApproval", execution.TaskStateWaitForApproval},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			exec := &execution.Execution{ID: "e-" + tc.name, State: tc.state}
			require.NoError(t, rt.SaveExecution(ctx, exec))

			out, err := rt.WaitForExecution(ctx, exec.ID, 100*time.Millisecond)
			require.NoError(t, err)
			require.EqualValues(t, exec, out)
		})
	}
}
