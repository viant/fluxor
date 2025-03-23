package allocator

import (
	"fmt"
	"github.com/viant/fluxor/model/evaluator"
	"github.com/viant/fluxor/model/execution"
	"github.com/viant/fluxor/model/graph"
	"strings"
)

// evaluateCondition evaluates a simple condition string
// This is a placeholder for a more sophisticated condition evaluator
func evaluateCondition(condition string, process *execution.Process, task *graph.Task, anExecution *execution.Execution, defaultValue bool) (bool, error) {
	if condition == "" {
		return defaultValue, nil
	}

	condition = strings.TrimPrefix(condition, "${")
	condition = strings.TrimSuffix(condition, "}")

	session := process.Session.Clone()
	session.Set(task.Namespace, anExecution.Output)

	evaluated := evaluator.Evaluate(condition, session.State)
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
