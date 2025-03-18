package processor

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/viant/fluxor/model"
	"github.com/viant/fluxor/model/execution"
	"github.com/viant/fluxor/model/graph"
	"github.com/viant/fluxor/model/state"
	"github.com/viant/fluxor/service/messaging/memory"
	"testing"
	"time"
)

func TestService_StartProcess(t *testing.T) {
	// Create a simple workflow
	workflow := &model.Workflow{
		Name: "TestWorkflow",
		Init: state.Parameters{
			{Name: "i", Value: 0},
		},
		Pipeline: &graph.Task{
			Tasks: []*graph.Task{
				{
					ID: "task",
					Tasks: []*graph.Task{
						{
							ID: "subtask",
							Action: &graph.Action{
								Service: "printer",
								Method:  "print",
								Input: map[string]interface{}{
									"message": "Hello  ${i}",
								},
							},
						},
					},
				},
			},
		},
	}

	testCases := []struct {
		name       string
		customPath []string
		expectErr  bool
	}{
		{
			name:       "Default execution order",
			customPath: nil,
			expectErr:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create executor service with memory queue
			queue := memory.NewQueue[execution.Execution](memory.DefaultConfig())

			executor, err := New(WithMessageQueue(queue), WithWorkers(1))
			assert.Nil(t, err)
			// Start the process
			ctx := context.Background()
			process, err := executor.StartProcess(ctx, workflow, tc.customPath)

			if tc.expectErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, process)
			assert.Equal(t, execution.StateRunning, process.GetState())

			// Allow some time for tasks to be processed
			time.Sleep(100 * time.Millisecond)

			// Test process management functions
			err = executor.PauseProcess(ctx, process.ID)
			assert.NoError(t, err)
			assert.Equal(t, execution.StatePaused, process.GetState())

			err = executor.ResumeProcess(ctx, process.ID)
			assert.NoError(t, err)
			assert.Equal(t, execution.StateRunning, process.GetState())

			// Check process retrieval
			retrieved, err := executor.GetProcess(ctx, process.ID)
			assert.NoError(t, err)
			assert.Equal(t, process.ID, retrieved.ID)

			// Cleanup
			executor.Shutdown()
		})
	}
}
