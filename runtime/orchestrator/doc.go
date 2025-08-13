// Package orchestrator exposes a lightweight programmatic emit/await API for
// task actions. It allows tasks (e.g., a ReAct plan step) to fan-out child
// executions and optionally await their completion without modifying the
// workflow YAML. The orchestrator is injected into the execution context and
// retrieved in the action via ctx.Value(orchestrator.OrchestratorContextKey).
package orchestrator
