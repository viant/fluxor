package main

import (
	"encoding/json"
	"fmt"
	"github.com/viant/fluxor/model"
)

func main() {
	// Create a workflow with stages as in the issue description
	workflow := createWorkflowWithStages()

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

// createWorkflowWithStages creates a workflow with stages as described in the issue
func createWorkflowWithStages() *model.Workflow {
	// Create a new workflow
	workflow := model.NewWorkflow("agent")

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

	return workflow
}
