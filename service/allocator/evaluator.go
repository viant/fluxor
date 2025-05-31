package allocator

import (
	"fmt"
	"github.com/viant/fluxor/model/graph"
	"github.com/viant/fluxor/runtime/evaluator"
	execution2 "github.com/viant/fluxor/runtime/execution"
	"strings"
)

// evaluateCondition evaluates a simple condition string
// This is a placeholder for a more sophisticated condition evaluator
func evaluateCondition(condition string, process *execution2.Process, task *graph.Task, anExecution *execution2.Execution, defaultValue bool) (bool, error) {
	if condition == "" {
		return defaultValue, nil
	}

	session := process.Session.Clone()
	if anExecution.Output != nil {
		session.Set(task.Namespace, anExecution.Output)
	}
	evaluated := evaluator.Evaluate(condition, session.State)
	if evaluated == nil {
		return false, nil
	}
	switch actual := evaluated.(type) {
	case bool:
		return actual, nil
	case int:
		return actual != 0, nil
	case string:
		return strings.TrimSpace(actual) != "", nil
	case float64:
		return actual != 0, nil
	case float32:
		return actual != 0, nil
	default:
		return false, fmt.Errorf("unsupported condition type: %T +%v\n", evaluated, evaluated)
	}
}
