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
	"fmt"
	"github.com/viant/fluxor"
	"time"
)

func main() {
	err := runIt()
	if err != nil {
		panic(err)
	}
}

func runIt() error {
	srv := fluxor.New()
	runtime := srv.Runtime()
	ctx := context.Background()
	workflow, err := runtime.LoadWorkflow(ctx, "parent.yaml")
	if err != nil {
		return err
	}
	_ = runtime.Start(ctx)
	process, wait, err := runtime.StartProcess(ctx, workflow, map[string]interface{}{})
	if err != nil {
		return err
	}
	fmt.Println("process:", process.ID)
	output, err := wait(ctx, time.Minute)
	if err != nil {
		return err
	}
	fmt.Printf("output: %+v\n", output)
	return nil
}
```


## Defining Workflows

Fluxor workflows are defined in YAML or JSON. Here's a simple example:

```yaml
init:
  i: 0

pipeline:
  start:
    action: printer:print
    input:
      message: 'Parent started'
  loop:
    inc:
      action: nop:nop
      post:
        i: ${i + 1}

    runChildren:
      action: workflow:run
      input:
        location: children
        context:
          iteration: $i
    body:
      action: printer:print
      input:
        message: 'Iteration: $i'
      goto:
        when: i < 3
        task: loop

  stop:
    action: printer:print
    input:
      message: 'Parent stoped'

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

### Template Tasks

Template tasks allow you to repeat a sub-task over each element of a collection. Use the `template` keyword with a `selector` and an inner `task` definition:

```yaml
pipeline:
  processOrders:
    template:
      selector:
        - name: order
          value: "$orders"
      task:
        processOne:
          action:
            service: printer
            method: print
          input:
            message: "Order: $order"
```

In this example, `processOrders` will spawn one `processOne` task for each element in `orders`, binding the current element to `$order` in each task.


## Contributing

Contributions to Fluxor are welcome! Please feel free to submit a Pull Request.

## License

Fluxor is licensed under the [LICENSE](LICENSE) file in the root directory of this source tree.

---

© 2012-2023 Viant, inc. All rights reserved.
