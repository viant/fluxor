package approval

// Request represents a request for user approval of a specific execution
// before the task action can be carried out.
type Request struct {
	ExecutionID string                 `json:"executionId"`
	Action      string                 `json:"action"`         // fully-qualified service.method
	Args        map[string]interface{} `json:"args,omitempty"` // expanded input parameters (best-effort)
}
