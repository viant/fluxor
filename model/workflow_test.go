package model

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"github.com/viant/fluxor/model/graph"
	"testing"
)

func TestProgrammaticWorkflowCreation(t *testing.T) {
	// Create a new workflow
	workflow := NewWorkflow("agent")

	// Create the "plan" task
	planTask := workflow.NewTask("plan")
	planTask.WithAction("llm", "plan", map[string]interface{}{
		"query":   "${query}",
		"context": "${context}",
		"model":   "${model}",
		"tools":   "${sequence}",
	})
	planTask.WithPost("plan", "${plan}")

	// Create the "exec" task
	execTask := workflow.NewTask("exec")
	execTask.WithAction("tool", "executePlan", map[string]interface{}{
		"plan":  "${plan}",
		"model": "${model}",
	})
	execTask.WithPost("results[]", "${results}")

	// Create the "decide" task
	decideTask := workflow.NewTask("decide")
	decideTask.WithAction("llm", "plan", map[string]interface{}{
		"query":   "${query}",
		"context": "${context}",
		"results": "${results}",
		"model":   "${model}",
		"tools":   "${sequence}",
	})
	decideTask.WithPost("plan", "${plan}")
	decideTask.WithGoto("${len(plan) > 0}", "exec")

	// Create the "finish" task
	finishTask := workflow.NewTask("finish")
	finishTask.WithAction("llm", "finalize", map[string]interface{}{
		"query":   "${query}",
		"context": "${context}",
		"results": "${results}",
		"model":   "${model}",
	})
	finishTask.WithPost("answer", "${answer}")

	// Convert to JSON for comparison
	workflowJSON, err := json.MarshalIndent(workflow, "", "  ")
	assert.NoError(t, err)

	// Print the JSON for debugging
	t.Logf("Workflow JSON: %s", workflowJSON)

	// Verify the workflow structure
	assert.Equal(t, "agent", workflow.Name)
	assert.Len(t, workflow.Pipeline.Tasks, 4)

	// Verify the "plan" task
	assert.Equal(t, "plan", workflow.Pipeline.Tasks[0].Name)
	assert.Equal(t, "llm", workflow.Pipeline.Tasks[0].Action.Service)
	assert.Equal(t, "plan", workflow.Pipeline.Tasks[0].Action.Method)

	// Verify the "exec" task
	assert.Equal(t, "exec", workflow.Pipeline.Tasks[1].Name)
	assert.Equal(t, "tool", workflow.Pipeline.Tasks[1].Action.Service)
	assert.Equal(t, "executePlan", workflow.Pipeline.Tasks[1].Action.Method)

	// Verify the "decide" task
	assert.Equal(t, "decide", workflow.Pipeline.Tasks[2].Name)
	assert.Equal(t, "llm", workflow.Pipeline.Tasks[2].Action.Service)
	assert.Equal(t, "plan", workflow.Pipeline.Tasks[2].Action.Method)
	assert.Len(t, workflow.Pipeline.Tasks[2].Goto, 1)
	assert.Equal(t, "${len(plan) > 0}", workflow.Pipeline.Tasks[2].Goto[0].When)
	assert.Equal(t, "exec", workflow.Pipeline.Tasks[2].Goto[0].Task)

	// Verify the "finish" task
	assert.Equal(t, "finish", workflow.Pipeline.Tasks[3].Name)
	assert.Equal(t, "llm", workflow.Pipeline.Tasks[3].Action.Service)
	assert.Equal(t, "finalize", workflow.Pipeline.Tasks[3].Action.Method)
}

// TestProgrammaticWorkflowCreationWithStages demonstrates creating a workflow with stages
func TestProgrammaticWorkflowCreationWithStages(t *testing.T) {
	// Create a new workflow
	workflow := NewWorkflow("agent")

	// Create a stage task
	stageTask := workflow.NewTask("stage")

	// Create the "plan" task as a subtask of stage
	planTask := stageTask.AddSubTask("plan")
	planTask.WithAction("llm", "plan", map[string]interface{}{
		"query":   "${query}",
		"context": "${context}",
		"model":   "${model}",
		"tools":   "${sequence}",
	})
	planTask.WithPost("plan", "${plan}")

	// Create the "exec" task as a subtask of stage
	execTask := stageTask.AddSubTask("exec")
	execTask.WithAction("tool", "executePlan", map[string]interface{}{
		"plan":  "${plan}",
		"model": "${model}",
	})
	execTask.WithPost("results[]", "${results}")

	// Create the "decide" task as a subtask of stage
	decideTask := stageTask.AddSubTask("decide")
	decideTask.WithAction("llm", "plan", map[string]interface{}{
		"query":   "${query}",
		"context": "${context}",
		"results": "${results}",
		"model":   "${model}",
		"tools":   "${sequence}",
	})
	decideTask.WithPost("plan", "${plan}")
	decideTask.WithGoto("${len(plan) > 0}", "exec")

	// Create the "finish" task as a subtask of stage
	finishTask := stageTask.AddSubTask("finish")
	finishTask.WithAction("llm", "finalize", map[string]interface{}{
		"query":   "${query}",
		"context": "${context}",
		"results": "${results}",
		"model":   "${model}",
	})
	finishTask.WithPost("answer", "${answer}")

	// Convert to JSON for comparison
	workflowJSON, err := json.MarshalIndent(workflow, "", "  ")
	assert.NoError(t, err)

	// Print the JSON for debugging
	t.Logf("Workflow JSON: %s", workflowJSON)

	// Verify the workflow structure
	assert.Equal(t, "agent", workflow.Name)
	assert.Len(t, workflow.Pipeline.Tasks, 1)
	assert.Equal(t, "stage", workflow.Pipeline.Tasks[0].Name)
	assert.Len(t, workflow.Pipeline.Tasks[0].Tasks, 4)

	// Verify the "plan" task
	assert.Equal(t, "plan", workflow.Pipeline.Tasks[0].Tasks[0].Name)
	assert.Equal(t, "llm", workflow.Pipeline.Tasks[0].Tasks[0].Action.Service)
	assert.Equal(t, "plan", workflow.Pipeline.Tasks[0].Tasks[0].Action.Method)

	// Verify the "exec" task
	assert.Equal(t, "exec", workflow.Pipeline.Tasks[0].Tasks[1].Name)
	assert.Equal(t, "tool", workflow.Pipeline.Tasks[0].Tasks[1].Action.Service)
	assert.Equal(t, "executePlan", workflow.Pipeline.Tasks[0].Tasks[1].Action.Method)

	// Verify the "decide" task
	assert.Equal(t, "decide", workflow.Pipeline.Tasks[0].Tasks[2].Name)
	assert.Equal(t, "llm", workflow.Pipeline.Tasks[0].Tasks[2].Action.Service)
	assert.Equal(t, "plan", workflow.Pipeline.Tasks[0].Tasks[2].Action.Method)
	assert.Len(t, workflow.Pipeline.Tasks[0].Tasks[2].Goto, 1)
	assert.Equal(t, "${len(plan) > 0}", workflow.Pipeline.Tasks[0].Tasks[2].Goto[0].When)
	assert.Equal(t, "exec", workflow.Pipeline.Tasks[0].Tasks[2].Goto[0].Task)

	// Verify the "finish" task
	assert.Equal(t, "finish", workflow.Pipeline.Tasks[0].Tasks[3].Name)
	assert.Equal(t, "llm", workflow.Pipeline.Tasks[0].Tasks[3].Action.Service)
	assert.Equal(t, "finalize", workflow.Pipeline.Tasks[0].Tasks[3].Action.Method)
}

// TestProgrammaticWorkflowCreationWithCreateSubTask demonstrates creating a workflow with stages using CreateSubTask
func TestProgrammaticWorkflowCreationWithCreateSubTask(t *testing.T) {
	// Create a new workflow
	workflow := NewWorkflow("agent")

	// Create a stage task
	stageTask := workflow.NewTask("stage")

	// Create the "plan" task as a subtask of stage using CreateSubTask
	stageTask.CreateSubTask("plan",
		func(t *graph.Task) *graph.Task {
			return t.WithAction("llm", "plan", map[string]interface{}{
				"query":   "${query}",
				"context": "${context}",
				"model":   "${model}",
				"tools":   "${sequence}",
			})
		},
		func(t *graph.Task) *graph.Task {
			return t.WithPost("plan", "${plan}")
		},
	)

	// Create the "exec" task as a subtask of stage using CreateSubTask
	stageTask.CreateSubTask("exec",
		func(t *graph.Task) *graph.Task {
			return t.WithAction("tool", "executePlan", map[string]interface{}{
				"plan":  "${plan}",
				"model": "${model}",
			})
		},
		func(t *graph.Task) *graph.Task {
			return t.WithPost("results[]", "${results}")
		},
	)

	// Create the "decide" task as a subtask of stage using CreateSubTask
	stageTask.CreateSubTask("decide",
		func(t *graph.Task) *graph.Task {
			return t.WithAction("llm", "plan", map[string]interface{}{
				"query":   "${query}",
				"context": "${context}",
				"results": "${results}",
				"model":   "${model}",
				"tools":   "${sequence}",
			})
		},
		func(t *graph.Task) *graph.Task {
			return t.WithPost("plan", "${plan}")
		},
		func(t *graph.Task) *graph.Task {
			return t.WithGoto("${len(plan) > 0}", "exec")
		},
	)

	// Create the "finish" task as a subtask of stage using CreateSubTask
	stageTask.CreateSubTask("finish",
		func(t *graph.Task) *graph.Task {
			return t.WithAction("llm", "finalize", map[string]interface{}{
				"query":   "${query}",
				"context": "${context}",
				"results": "${results}",
				"model":   "${model}",
			})
		},
		func(t *graph.Task) *graph.Task {
			return t.WithPost("answer", "${answer}")
		},
	)

	// Convert to JSON for comparison
	workflowJSON, err := json.MarshalIndent(workflow, "", "  ")
	assert.NoError(t, err)

	// Print the JSON for debugging
	t.Logf("Workflow JSON: %s", workflowJSON)

	// Verify the workflow structure
	assert.Equal(t, "agent", workflow.Name)
	assert.Len(t, workflow.Pipeline.Tasks, 1)
	assert.Equal(t, "stage", workflow.Pipeline.Tasks[0].Name)
	assert.Len(t, workflow.Pipeline.Tasks[0].Tasks, 4)

	// Verify the "plan" task
	assert.Equal(t, "plan", workflow.Pipeline.Tasks[0].Tasks[0].Name)
	assert.Equal(t, "llm", workflow.Pipeline.Tasks[0].Tasks[0].Action.Service)
	assert.Equal(t, "plan", workflow.Pipeline.Tasks[0].Tasks[0].Action.Method)

	// Verify the "exec" task
	assert.Equal(t, "exec", workflow.Pipeline.Tasks[0].Tasks[1].Name)
	assert.Equal(t, "tool", workflow.Pipeline.Tasks[0].Tasks[1].Action.Service)
	assert.Equal(t, "executePlan", workflow.Pipeline.Tasks[0].Tasks[1].Action.Method)

	// Verify the "decide" task
	assert.Equal(t, "decide", workflow.Pipeline.Tasks[0].Tasks[2].Name)
	assert.Equal(t, "llm", workflow.Pipeline.Tasks[0].Tasks[2].Action.Service)
	assert.Equal(t, "plan", workflow.Pipeline.Tasks[0].Tasks[2].Action.Method)
	assert.Len(t, workflow.Pipeline.Tasks[0].Tasks[2].Goto, 1)
	assert.Equal(t, "${len(plan) > 0}", workflow.Pipeline.Tasks[0].Tasks[2].Goto[0].When)
	assert.Equal(t, "exec", workflow.Pipeline.Tasks[0].Tasks[2].Goto[0].Task)

	// Verify the "finish" task
	assert.Equal(t, "finish", workflow.Pipeline.Tasks[0].Tasks[3].Name)
	assert.Equal(t, "llm", workflow.Pipeline.Tasks[0].Tasks[3].Action.Service)
	assert.Equal(t, "finalize", workflow.Pipeline.Tasks[0].Tasks[3].Action.Method)
}
