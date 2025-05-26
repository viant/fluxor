# Programmatic Workflow Creation Example

This example demonstrates how to create a workflow programmatically using the Fluxor API.

## Overview

The example creates a workflow with a structure similar to the following YAML:

```yaml
stage:
  plan:
    action: llm:plan
    input:
      query: ${query}
      context: ${context}
      model: ${model}
      tools: ${sequence}
    post:
      plan: ${plan}

  exec:
    action: tool:executePlan
    input:
      plan: ${plan}
      model: ${model}
    post:
      'results[]': ${results}

  decide:
    action: llm:plan
    input:
      query: ${query}
      context: ${context}
      results: ${results}
      model: ${model}
      tools: ${sequence}
    post:
      plan: ${plan}
    goto:
      when: ${len(plan) > 0}
      task: exec

  finish:
    action: llm:finalize
    input:
      query: ${query}
      context: ${context}
      results: ${results}
      model: ${model}
    post:
      answer: ${answer}
```

## Usage

The example demonstrates how to:

1. Create a new workflow using `model.NewWorkflow()`
2. Add tasks to the workflow using `workflow.NewTask()`
3. Add subtasks to tasks using `task.AddSubTask()`
4. Configure tasks with actions using `task.WithAction()`
5. Add post-execution parameters using `task.WithPost()`
6. Add transitions between tasks using `task.WithGoto()`

## Running the Example

To run the example:

```bash
cd examples/workflow
go run main.go
```

This will output the JSON representation of the created workflow.

## API Reference

### Creating a Workflow

```go
// Create a new workflow
workflow := model.NewWorkflow("workflowName")
```

### Adding Tasks

```go
// Add a top-level task
task := workflow.NewTask("taskName")

// Add a subtask
subtask := task.AddSubTask("subtaskName")
```

### Configuring Tasks

```go
// Add an action
task.WithAction("service", "method", inputMap)

// Add initialization parameters
task.WithInit("paramName", paramValue)

// Add post-execution parameters
task.WithPost("paramName", paramValue)

// Add a transition
task.WithGoto("condition", "targetTaskName")

// Set task to run asynchronously
task.WithAsync(true)

// Set auto-pause flag
task.WithAutoPause(true)
```

### Configuring Workflows

```go
// Add a description
workflow.WithDescription("description")

// Add a version
workflow.WithVersion("1.0.0")

// Add initialization parameters
workflow.WithInit("paramName", paramValue)

// Add post-execution parameters
workflow.WithPost("paramName", paramValue)

// Add configuration parameters
workflow.WithConfig("key", value)
```