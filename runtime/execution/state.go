package execution

// TaskState represents the current State of a task
type TaskState string

const (
	TaskStatePending             TaskState = "pending"
	TaskStateScheduled           TaskState = "scheduled"
	TaskStateRunning             TaskState = "running"
	TaskStateWaitForDependencies TaskState = "waitForDependencies" //waiting for dependency
	TaskStateWaitForSubTasks     TaskState = "waitForSubTasks"     //waiting for subtask
	// TaskStateWaitForApproval indicates the task is waiting for explicit
	// approval before it can be executed.  Used by the optional policy/approval
	// mechanism.
	TaskStateWaitForApproval TaskState = "waitForApproval"
	TaskStateCompleted       TaskState = "completed"
	TaskStateFailed          TaskState = "failed"
	TaskStatePaused          TaskState = "paused"

	TaskStateSkipped TaskState = "skipped"
)

func (t TaskState) IsWaitForApproval() bool {
	return t == TaskStateWaitForApproval
}
