package event

import "time"

type Context struct {
	ProcessID   string `json:"processID"`
	TaskID      string `json:"taskID"`
	EventType   string `json:"eventType"`
	Service     string `json:"service"`
	Method      string `json:"method"`
	TimeTakenMs int    `json:"timeTakenMs"`
}

type Event[T any] struct {
	Context   *Context               `json:"context"`
	CreatedAt time.Time              `json:"createdAt"`
	Metadata  map[string]interface{} `json:"metadata"`
	Data      T                      `json:"data"`
}

func NewEvent[T any](context *Context, data T) *Event[T] {
	return &Event[T]{
		Context:   context,
		CreatedAt: time.Now(),
		Metadata:  make(map[string]interface{}),
		Data:      data,
	}
}
