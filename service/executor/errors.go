package executor

import "errors"

var (
	ErrTaskNotFound   = errors.New("task not found in workflow")
	ErrMethodNotFound = errors.New("method not found in service")
)
