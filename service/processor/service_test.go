package processor

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/viant/fluxor/extension"
	"github.com/viant/fluxor/model"
	"github.com/viant/fluxor/model/graph"
	"github.com/viant/fluxor/model/state"
	execution2 "github.com/viant/fluxor/runtime/execution"
	"github.com/viant/fluxor/service/action/printer"
	execMemory "github.com/viant/fluxor/service/dao/execution/memory"
	processMemory "github.com/viant/fluxor/service/dao/process/memory"
	"github.com/viant/fluxor/service/executor"
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
		name        string
		customTasks []string
		expectErr   bool
	}{
		{
			name:        "Default execution order",
			customTasks: nil,
			expectErr:   false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create executor service with memory queue
			queue := memory.NewQueue[execution2.Execution](memory.DefaultConfig())

			// Create memory DAOs
			processDAO := processMemory.New()
			executionDAO := execMemory.New()

			// Create actions registry and register printer service
			actions := extension.NewActions()
			actions.Register(printer.New())

			// Create executor service
			execService := executor.NewService(actions)

			// Create context
			ctx := context.Background()

			// Create processor service with all required dependencies
			processor, err := New(
				WithMessageQueue(queue),
				WithWorkers(100),
				WithProcessDAO(processDAO),
				WithTaskExecutionDAO(executionDAO),
				WithExecutor(execService),
			)
			assert.Nil(t, err)

			// Start the processor service
			err = processor.Start(ctx)
			assert.NoError(t, err)

			// Start the process
			process, err := processor.StartProcess(ctx, workflow, map[string]interface{}{}, tc.customTasks...)

			if tc.expectErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, process)
			assert.Equal(t, execution2.StateRunning, process.GetState())

			// Allow some time for tasks to be processed
			time.Sleep(100 * time.Millisecond)

			// Test process management functions
			err = processor.PauseProcess(ctx, process.ID)
			assert.NoError(t, err)
			assert.Equal(t, execution2.StatePaused, process.GetState())

			err = processor.ResumeProcess(ctx, process.ID)
			assert.NoError(t, err)
			assert.Equal(t, execution2.StateRunning, process.GetState())

			// Check process retrieval
			retrieved, err := processor.GetProcess(ctx, process.ID)
			assert.NoError(t, err)
			assert.Equal(t, process.ID, retrieved.ID)

			// Cleanup
			processor.Shutdown()
		})
	}
}
