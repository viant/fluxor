package execution

// TaskState represents the current State of a task
type TaskState string

const (
	TaskStatePending             TaskState = "pending"
	TaskStateScheduled           TaskState = "scheduled"
	TaskStateRunning             TaskState = "running"
	TaskStateWaitForDependencies TaskState = "waitForDependencies" //waiting for dependency
	TaskStateWaitForSubTasks     TaskState = "waitForSubTasks"     //waiting for subtask
	TaskStateCompleted           TaskState = "completed"
	TaskStateFailed              TaskState = "failed"
	TaskStatePaused              TaskState = "paused"

	TaskStateSkipped TaskState = "skipped"
)
