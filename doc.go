// Package fluxor provides a generic, extensible workflow engine.
//
// The engine executes workflows defined declaratively (for example in YAML
// or JSON) and comes with pluggable service layers such as:
//
//   - runtime  – orchestration of workflow execution
//   - allocator – task allocation and state management
//   - executor  – task execution through custom actions
//   - approval  – optional human-in-the-loop task approval
//
// Fluxor is designed to be embedded in host applications.  End-users
// typically interact with the engine via the high-level Service façade
// exposed by the root package:
//
//	srv := fluxor.New()
//	rt  := srv.Runtime()
//	wf, _ := rt.LoadWorkflow(ctx, "workflow.yaml")
//	_, wait, _ := rt.StartProcess(ctx, wf, nil)
//	out, _ := wait(ctx, time.Minute)
//
// For more details see the README and individual sub-packages.
package fluxor
