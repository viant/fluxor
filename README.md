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
- [Optional Task Approval & Policy Layer](#optional-task-approval--policy-layer)
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
- **Optional Human Approval**: Pause selected tasks until a human (or custom logic) approves them
- **Ad-hoc Execution**: Run a single task or sub-pipeline on demand for quick streaming, tests, debugging or manual reruns without launching the full workflow

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

                              │ (approval request)
                              ▼
                       ┌─────────────────┐
                       │  Approval Queue │
                       └────────┬────────┘
                                │ consume
                                ▼
                       ┌─────────────────┐
                       │  Approval       │
                       │   Service       │
                       └─────────────────┘
```

The Approval components are initialised only when the **optional policy layer** (see below) is enabled.

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

Alternatively, you can define the pipeline as an ordered sequence of task definitions:

```yaml
pipeline:
  - id: list
    service: system/storage
    action: list
    with:
      URL: "file://."
  - id: show
    service: printer
    action: print
    with:
      message: ${ $.list.entries }
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
            Name:        "execute",
            Description: "Executes custom action logic using provided input parameters to produce output.",
            Input:       reflect.TypeOf(&Input{}),
            Output:      reflect.TypeOf(&Output{}),
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
              message: $order
```

In this example, `processOrders` will spawn one `processOne` task for each element in `orders`, binding the current element to `$order` in each task.

⚠️  Runtime validation: at the moment the selector expression is expanded the
resolved value **must** be a slice or array. If it evaluates to any other
type (string, map, scalar, nil, …) Fluxor fails the task with an explicit
error message and the workflow moves to its normal failure path (or retries
if a retry-strategy is defined).



### Ad-hoc Execution

  Sometimes you only want to run *one* task (or a small subset of the pipeline)
  to verify behaviour, re-process a failed item or perform a quick manual job.
  Fluxor exposes a convenience helper that executes a task outside the context
of a full workflow run:

  ```go
  taskID  := "transformCsv"
input   := map[string]any{"file": "customers.csv"}

  // Execute the task once and wait synchronously for the result.
  output, err := runtime.RunTaskOnce(ctx, workflow, taskID, input)
  if err != nil {
  log.Fatal(err)
}
fmt.Printf("%s output: %+v\n", taskID, output)
```

Under the hood the engine still uses the same allocator / processor / executor
infrastructure so semantics stay identical⁠—you just avoid spinning up the
entire parent workflow.
input:
message: "Order: $order"


## Optional Task Approval & Policy Layer

Fluxor includes an **opt-in** policy subsystem that can pause task execution until it is manually approved – perfect for production safety-nets, dry-runs or interactive debugging.

## Runtime Control Plane (pause / resume / cancel)

Starting with v0.4 Fluxor ships a lightweight control-plane that lets operators interact with a **live** workflow run after it has started.

| Action | Effect |
|--------|--------|
| `pause`  | Stops **scheduling** new tasks. In-flight executions finish normally. |
| `resume` | Re-enables scheduling after a pause. |
| `cancel` | Immediately cancels the per-process context (<code>ctx.Done()</code>) so long-running tasks terminate early **and** stops further scheduling. | 
| `resumeFailed` | Rewinds a workflow that ended in *failed* / *cancelled* state and re-starts it. |

Simple API calls:

```go
// pause / resume
_ = processor.PauseProcess(ctx,   "workflow/123")
_ = processor.ResumeProcess(ctx,  "workflow/123")

// graceful cancellation
_ = processor.CancelProcess(ctx,  "workflow/123", "cli kill")

// give the workflow another try after fixing the root cause
_ = processor.ResumeFailedProcess(ctx, "workflow/123")
```

### Life-cycle diagram

```
          ┌────────────┐               ┌───────────────┐
   start  │  running   │ pause         │ pauseRequested│
─────────►│            │──────────────►│               │
          └─────┬──────┘               └──────┬────────┘
                │ resume                         │ all running
                │                                │ tasks drained
                │                                ▼
                │               ┌───────────────┐
                └───────────────│   paused      │
                                 └──────┬────────┘ resume
                                        │
                                        ▼
                                  (back to running)

cancel        ┌──────────────┐
─────────────►│cancelRequested│── allocator ──► cancelled
              └──────────────┘

```

## One-line Progress Tracker

Every root workflow carries a context-local *Progress* tracker.  Allocator, Processor and Executor update it with a single helper:

```go
progress.UpdateCtx(ctx, progress.Delta{Pending:-1, Running:+1})
```

The tracker keeps aggregate counters (total / completed / skipped / failed / …) for **all** sub-workflows and exposes an optional callback to stream live updates to dashboards.

Snapshot at any time:

```go
if p, ok := progress.GetSnapshot(ctx); ok {
    fmt.Printf("%.1f%% done (%d/%d)\n",
        100*float64(p.CompletedTasks)/float64(p.TotalTasks),
        p.CompletedTasks, p.TotalTasks)
}
```

### Execution Modes

| Mode | Behaviour |
|------|-----------|
| `auto` | Run tasks immediately (default). |
| `ask`  | Create an approval request before running each task (unless whitelisted). |
| `deny` | Skip the task and mark the execution as failed. |

### Minimal Example

```go
ctx := policy.WithPolicy(context.Background(), &policy.Policy{
    Mode: policy.ModeAsk,
})

proc, wait, _ := runtime.StartProcess(ctx, wf, nil)
// ... respond to approval prompts ...
```

### End-to-end Flow

1. The executor evaluates the policy before every task.
2. If `Mode==ask` and no decision has been recorded yet, it emits an `approval.Request` message and suspends the execution (`waitForApproval`).
3. The Approval Service consumes the request, calls your `AskFunc`, persists the decision and – if approved – re-enqueues the execution so a worker can continue processing.

The default `AskFunc` bundled with Fluxor simply returns `true` (auto-approve) so that enabling the feature never blocks development.  Provide your own callback via `fluxor.WithApprovalAskFunc` for real-world use cases.


## Contributing

Contributions to Fluxor are welcome! Please feel free to submit a Pull Request.

## License

Fluxor is licensed under the [LICENSE](LICENSE) file in the root directory of this source tree.

---

© 2012-2023 Viant, inc. All rights reserved.
