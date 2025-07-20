package types

import "context"

type executionContextKey string

// ExecutionContextKey execution context
var ExecutionContextKey = executionContextKey("execution-context")

// EnsureExecutionContext ensure
func EnsureExecutionContext(ctx context.Context, pairs ...string) context.Context {
	v := ctx.Value(ExecutionContextKey)
	if v == nil {
		ctx = context.WithValue(ctx, ExecutionContextKey, map[string]any{})
	}
	values := ctx.Value(ExecutionContextKey).(map[string]any)
	for i := 0; i < len(pairs); i += 2 {
		values[pairs[i]] = pairs[i+1]
	}
	return ctx
}
