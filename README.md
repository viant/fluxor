# Fluxor: A Generic Workflow Engine

Fluxor is a powerful, Go-based workflow engine that allows you to define, execute, and manage complex workflows with custom actions and executors. This README provides an overview of Fluxor's architecture, features, and how to use it effectively.

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [Getting Started](#getting-started)
- [Defining Workflows](#defining-workflows)
- [Custom Actions](#custom-actions)
- [Executors](#executors)
- [Task Allocation](#task-allocation)
- [Advanced Features](#advanced-features)
- [Contributing](#contributing)
- [License](#license)

## Overview

Fluxor is designed to provide a flexible, extensible workflow engine that can be integrated into any Go application. It supports:

- Complex workflow definitions with nested tasks
- Task dependencies and conditional execution
- Custom actions and executors
- State management and persistence
- Pausable and resumable workflows
- Dynamic task allocation

## Key Features

- **Declarative Workflow Definitions**: Define workflows in YAML or JSON
- **Task Dependencies**: Specify dependencies between tasks to control execution order
- **Custom Actions**: Create and register custom actions to extend the engine
- **State Management**: Track workflow state and execution history
- **Concurrency Control**: Control the level of parallelism in workflow execution
- **Extensible Architecture**: Easily extend the engine with custom components

## Architecture

Fluxor consists of the following main components:

- **Workflow Model**: Defines the structure of workflows, tasks, and transitions
- **Process Engine**: Manages the lifecycle of workflow processes
- **Task Executor**: Executes individual tasks within a workflow
- **Task Allocator**: Allocates tasks to execution workers
- **Persistence Layer**: Stores workflow definitions and execution state
- **Extension System**: Allows for custom actions and executors

### Component Diagram

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Workflow      │    │      Task       │    │   Extension     │
│   Definition    │───▶│    Allocator    │───▶│     System      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │                        │
                              ▼                        ▼
                       ┌─────────────────┐    ┌─────────────────┐
                       │     Process     │    │      Task       │
                       │     Engine      │◀───│     Executor    │
                       └─────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌─────────────────┐
                       │   Persistence   │
                       │      Layer      │
                       └─────────────────┘
```

## Getting Started

### Installation

```bash
go get github.com/viant/fluxor
```

### Basic Usage

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/afs"
	_ "github.com/viant/afs/embed"
	"github.com/viant/fluxor/extension"
	"github.com/viant/fluxor/model/execution"
	"github.com/viant/fluxor/service/action/system/executor"
	"github.com/viant/fluxor/service/allocator"
	ememory "github.com/viant/fluxor/service/dao/execution/memory"
	pmemory "github.com/viant/fluxor/service/dao/process/memory"
	"github.com/viant/fluxor/service/dao/workflow"
	texecutor "github.com/viant/fluxor/service/executor"
	mmemory "github.com/viant/fluxor/service/messaging/memory"
	"time"
	"github.com/viant/fluxor/service/meta"
	"github.com/viant/fluxor/service/processor"
)

func main() {
	runIt()
}

func runIt() error {
	ctx := context.Background()

	// Initialize components
	actions := extension.NewActions()

	// Register custom actions
	// actions.Register(myCustomAction)
	actions.Register(executor.New())


	fs := afs.New()
	metaService := meta.New(fs, "file:///workflowBaseDir")
	workflowDao := workflow.New(workflow.WithRootTaskNodeName("stage"), workflow.WithMetaService(metaService))



	queue := mmemory.NewQueue[execution.Execution](mmemory.DefaultConfig())
	processorDao := pmemory.New()
	taskExecutionDao := ememory.New()
	
	anExecutor := texecutor.NewService(actions)
	anAllocator := allocator.New(processorDao, taskExecutionDao, queue, allocator.DefaultConfig())
	aProcessor, err := processor.New(
		processor.WithTaskExecutor(anExecutor),
		processor.WithMessageQueue(queue),
		processor.WithWorkers(1),
		processor.WithTaskExecutionDAO(taskExecutionDao),
		processor.WithProcessDAO(processorDao))
	if err != nil {
		return fmt.Errorf("failed to create processor: %w", err)
	}

	if err = aProcessor.Start(ctx);err != nil {
        return err
    }
	var initialState = make(map[string]interface{})
	initialState["attr1"] = 1
	aWorkflow, err := workflowDao.Load(ctx, "intent/coding.yaml")
	_, err = aProcessor.StartProcess(ctx, aWorkflow, nil, initialState)
    if err != nil {
		return err
    }
	return anAllocator.Start(ctx)
}

```

## Defining Workflows

Fluxor workflows are defined in YAML or JSON. Here's a simple example:

```yaml
init:
  counter: 0

pipeline:
  task1:
    action: system/executor
    commands:
      - echo "Starting workflow"
    
  task2:
    dependsOn: task1
    action: printer:print
    input:
      message: "Processing step ${counter}"
    
  task3:
    dependsOn: task2
    action: system/executor
    commands:
      - echo "Finishing workflow"
    transitions:
      - when: "${counter < 5}"
        goto: task2
```

## Custom Actions

Custom actions allow you to extend Fluxor with your own functionality:

```go
package myaction

import (
    "context"
    "github.com/viant/fluxor/model/types"
    "reflect"
)

type Input struct {
    Param1 string `json:"param1"`
    Param2 int    `json:"param2"`
}

type Output struct {
    Result string `json:"result"`
}

type Service struct{}

func (s *Service) Name() string {
    return "my/action"
}

func (s *Service) Methods() types.Signatures {
    return []types.Signature{
        {
            Name:   "execute",
            Input:  reflect.TypeOf(&Input{}),
            Output: reflect.TypeOf(&Output{}),
        },
    }
}

func (s *Service) Method(name string) (types.Executable, error) {
    if name == "execute" {
        return s.execute, nil
    }
    return nil, types.NewMethodNotFoundError(name)
}

func (s *Service) execute(ctx context.Context, in, out interface{}) error {
    input := in.(*Input)
    output := out.(*Output)
    
    // Implement your action logic
    output.Result = fmt.Sprintf("Processed %s with value %d", input.Param1, input.Param2)
    
    return nil
}
```

Register your custom action:

```go
actions.Register(&myaction.Service{})
```

## Executors

Executors handle the execution of tasks within workflows:

```yaml
task:
  action: system/executor
  commands:
    - echo "Hello, World!"
```

The built-in `system/executor` action allows you to run shell commands.

## Task Allocation

The task allocator is responsible for assigning tasks to execution workers and managing dependencies between tasks. It ensures tasks are executed in the correct order based on their dependencies.

```go
alloc := allocator.New(
    processDAO,
    taskExecutionDAO,
    queue,
    allocator.DefaultConfig(),
)
go alloc.Start(ctx)
```

## Advanced Features

### Task Dependencies

```yaml
taskB:
  dependsOn: taskA
  action: printer:print
  input:
    message: "Task B depends on Task A"
```

### Conditional Execution

```yaml
taskA:
  action: printer:print
  input:
    message: "Checking condition"
  transitions:
    - when: "${result > 100}"
      goto: highValueTask
    - when: "${result <= 100}"
      goto: lowValueTask
```

### Parallel Execution

```yaml
parallelTasks:
  tasks:
    - id: task1
      async: true
      action: printer:print
      input:
        message: "Running in parallel 1"
    
    - id: task2
      async: true
      action: printer:print
      input:
        message: "Running in parallel 2"
```

### Process Management

```go
// Pause a running process
err := procSvc.PauseProcess(ctx, processID)

// Resume a paused process
err := procSvc.ResumeProcess(ctx, processID)

// Get process status
process, err := procSvc.GetProcess(ctx, processID)
```

## Contributing

Contributions to Fluxor are welcome! Please feel free to submit a Pull Request.

## License

Fluxor is licensed under the [LICENSE](LICENSE) file in the root directory of this source tree.

---

© 2012-2023 Viant, inc. All rights reserved.
