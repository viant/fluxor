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
	var result bool
	if evaluated != nil {
		switch actual := evaluated.(type) {
		case bool:
			result = actual
		case int:
			result = actual != 0
		case string:
			result = strings.TrimSpace(actual) != ""
		case float64:
			result = actual != 0
		case float32:
			result = actual != 0
		default:
			return false, fmt.Errorf("unsupported condition type: %T +%v\n", evaluated, evaluated)
		}
	}
	// notify listeners
	process.Session.FireWhen(condition, result)
	return result, nil
}
