package execution

// TaskState represents the current State of a task
type TaskState string

const (
	TaskStatePending             TaskState = "pending"
	TaskStateScheduled           TaskState = "scheduled"
	TaskStateRunning             TaskState = "running"
	TaskStateWaitForDependencies TaskState = "waitForDependencies" //waiting for dependency
	TaskStateWaitForSubTasks     TaskState = "waitForSubTasks"     //waiting for subtask
	// TaskStateWaitAsync indicates the task emitted asynchronous child
	// executions and is waiting until the rendez-vous condition is satisfied.
	TaskStateWaitAsync TaskState = "waitAsync"
	// TaskStateWaitForApproval indicates the task is waiting for explicit
	// approval before it can be executed.  Used by the optional policy/approval
	// mechanism.
	TaskStateWaitForApproval TaskState = "waitForApproval"
	TaskStateCompleted       TaskState = "completed"
	TaskStateFailed          TaskState = "failed"
	TaskStatePaused          TaskState = "paused"

	TaskStateSkipped TaskState = "skipped"
	// TaskStateCancelled indicates the execution has been cancelled.
	TaskStateCancelled TaskState = "cancelled"
)

func (t TaskState) IsWaitForApproval() bool {
	return t == TaskStateWaitForApproval
}
