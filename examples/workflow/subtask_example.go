package main

import (
	"encoding/json"
	"fmt"
	"github.com/viant/fluxor/model"
	"github.com/viant/fluxor/model/graph"
)

// Example showing how to use the CreateSubTask method
func ExampleCreateSubTask() {
	// Create a new workflow
	workflow := model.NewWorkflow("agent")

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

	// Convert to JSON for display
	workflowJSON, err := json.MarshalIndent(workflow, "", "  ")
	if err != nil {
		fmt.Printf("Error marshaling workflow to JSON: %v\n", err)
		return
	}

	// Print the JSON
	fmt.Println("Workflow JSON:")
	fmt.Println(string(workflowJSON))
}
