package approval

import (
	"encoding/json"
	"time"
)

// Event envelope reused from the previous sketch.
type Event struct {
	Topic string      // "request.new" | "decision.new"
	Data  interface{} // *Request or *Decision
}

// Request represents a request for approval
type Request struct {
	ID          string                 `json:"id"`                  // Globally unique, primary key
	ProcessID   string                 `json:"processId"`           // Refers to process.ID
	ExecutionID string                 `json:"executionId"`         // Refers to execution.ID
	Action      string                 `json:"action"`              // "service.method"
	Args        json.RawMessage        `json:"args,omitempty"`      // JSON-encoded expanded input, may be null
	CreatedAt   time.Time              `json:"createdAt"`           // RFC-3339 timestamp
	ExpiresAt   *time.Time             `json:"expiresAt,omitempty"` // Optional deadline
	Meta        map[string]interface{} `json:"meta,omitempty"`      // Free-form map: tenant, user, environment, etc.
}

// Decision represents approval decision
type Decision struct {
	ID        string    `json:"id"` // same as request.ID
	Approved  bool      `json:"approved"`
	Reason    string    `json:"reason,omitempty"`
	DecidedAt time.Time `json:"decidedAt"`
}
