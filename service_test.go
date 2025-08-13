package fluxor_test

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	_ "github.com/viant/afs/embed"
	"github.com/viant/fluxor"
	"github.com/viant/fluxor/model/types"
	"github.com/viant/fluxor/policy"
	"github.com/viant/fluxor/runtime/execution"
	"github.com/viant/fluxor/service/approval"
	"github.com/viant/fluxor/service/executor"
)

//go:embed testdata/*
var embedFS embed.FS

func TestServiceGoTo(t *testing.T) {
	srv := fluxor.New(
		fluxor.WithMetaFsOptions(&embedFS),
		fluxor.WithMetaBaseURL("embed:///testdata"),
		//fluxor.WithExecutorOptions(executor.WithListener(executor.StdoutListener)),
		fluxor.WithTracing("fluxor", "0.0.1", "span.txt"),
		fluxor.WithWhenListeners(func(s *execution.Session, key string, result bool) {
			//data, _ := json.Marshal(s.State)
			//fmt.Printf("When: key: '%v', when: '%v'\n", key, result)
			//fmt.Println("state:" + string(data))
		}),
		fluxor.WithStateListeners(func(s *execution.Session, key string, oldVal, newVal interface{}) {
			//fmt.Printf("State changed: key: '%v', from: %v: to: %v\n", key, oldVal, newVal)
		}),
	)

	runtime := srv.Runtime()
	ctx := context.Background()

	approvalSrv := srv.ApprovalService()
	done := approval.AutoApprove(ctx, approvalSrv, 10*time.Millisecond)
	defer done()

	//ctx = policy.WithPolicy(ctx, &policy.Policy{ // only to copy into process
	//	Mode: policy.ModeAsk,
	//})

	workflow, err := runtime.LoadWorkflow(ctx, "goto.yaml")
	if !assert.Nil(t, err) {
		return
	}
	assert.NotNil(t, workflow)
	_ = runtime.Start(ctx)

	//ctx = policy.WithPolicy(ctx, &policy.Policy{ // only to copy into process
	//	Mode: policy.ModeAsk,
	//})

	process, wait, err := runtime.StartProcess(ctx, workflow, map[string]interface{}{})
	if !assert.Nil(t, err) {
		return
	}
	assert.NotNil(t, process)

	output, err := wait(ctx, time.Hour)
	data, _ := json.Marshal(output)
	fmt.Println(string(data), err)

}
func TestServiceWithParentWorkflow(t *testing.T) {
	srv := fluxor.New(
		fluxor.WithMetaFsOptions(&embedFS),
		fluxor.WithMetaBaseURL("embed:///testdata"),
		//fluxor.WithExecutorOptions(executor.WithListener(executor.StdoutListener)),
		fluxor.WithTracing("fluxor", "0.0.1", "span.txt"),
		fluxor.WithStateListeners(func(s *execution.Session, key string, oldVal, newVal interface{}) {
			//fmt.Printf("State changed: key: '%v', from: %v: to: %v\n", key, oldVal, newVal)
		}),
	)

	runtime := srv.Runtime()
	ctx := context.Background()

	approvalSrv := srv.ApprovalService()
	done := approval.AutoReject(ctx, approvalSrv, "", 10*time.Millisecond)
	defer done()
	workflow, err := runtime.LoadWorkflow(ctx, "parent.yaml")
	if !assert.Nil(t, err) {
		return
	}
	assert.NotNil(t, workflow)
	_ = runtime.Start(ctx)

	//ctx = policy.WithPolicy(ctx, &policy.Policy{ // only to copy into process
	//	Mode: policy.ModeAsk,
	//})

	process, wait, err := runtime.StartProcess(ctx, workflow, map[string]interface{}{})
	if !assert.Nil(t, err) {
		return
	}
	assert.NotNil(t, process)

	output, err := wait(ctx, time.Hour)
	data, _ := json.Marshal(output)
	fmt.Println(string(data), err)

}

// TestTemplate verifies the template feature by printing each order
func TestTemplate(t *testing.T) {
	// set up Fluxor service with embedded testdata
	srv := fluxor.New(
		fluxor.WithMetaFsOptions(&embedFS),
		fluxor.WithMetaBaseURL("embed:///testdata"),
		fluxor.WithExecutorOptions(executor.WithApprovalSkipPrefixes("system.storage")),
		//fluxor.WithStateListeners(func(s *execution.Session, key string, oldVal, newVal interface{}) {
		//	fmt.Printf("State changewith d: key: '%v', from: %v: to: %v\n", key, oldVal, newVal)
		//}),
	)

	runtime := srv.Runtime()
	ctx := context.Background()
	// Load the template workflow
	workflow, err := runtime.LoadWorkflow(ctx, "template.yaml")
	if !assert.Nil(t, err) {
		return
	}
	assert.NotNil(t, workflow)
	// start runtime (processor+allocator)
	_ = runtime.Start(ctx)

	approvalSrv := srv.ApprovalService()
	done := approval.AutoApprove(ctx, approvalSrv, 10*time.Millisecond)
	defer done()
	//
	ctx = policy.WithPolicy(ctx, &policy.Policy{ // only to copy into process
		Mode: policy.ModeAsk,
	})
	// Start the process with initial orders
	process, wait, err := runtime.StartProcess(ctx, workflow, map[string]interface{}{
		"orders": []interface{}{"apple", "banana", "cherry"},
	})
	if !assert.Nil(t, err) {
		return
	}
	assert.NotNil(t, process)

	// Wait for completion
	output, err := wait(ctx, time.Second)
	if !assert.Nil(t, err) {
		return
	}
	fmt.Println("done")
	// Process should complete without errors
	assert.EqualValues(t, execution.StateCompleted, output.State)
}

// ---------------------------------------------------------------------------
// Async emit/await E2E test
// ---------------------------------------------------------------------------

type addSvc struct{}

func (s *addSvc) Name() string { return "math/add" }

func (s *addSvc) Methods() types.Signatures {
	return types.Signatures{{
		Name:   "exec",
		Input:  reflect.TypeOf(map[string]interface{}{}),
		Output: reflect.TypeOf((*interface{})(nil)).Elem(),
	}}
}

func (s *addSvc) Method(name string) (types.Executable, error) {
	return func(ctx context.Context, in, out interface{}) error {
		m := in.(*map[string]interface{})
		a := int((*m)["a"].(float64))
		b := int((*m)["b"].(float64))
		sum := a + b
		ptr := out.(*interface{})
		*ptr = sum
		return nil
	}, nil
}

func TestAsyncEmitAwait(t *testing.T) {
	srv := fluxor.New()
	// register test add service
	srv.Actions().Register(&addSvc{})

	rt := srv.Runtime()
	ctx := context.Background()

	yaml := []byte(`
name: asyncAdd
pipeline:
  tasks:
    parent:
      emit:
        forEach: ${numbers}
        as: p
        task:
          add:
            action: math/add:exec
            input:
              a: ${p.a}
              b: ${p.b}
      await: true
`)

	assert.NoError(t, rt.UpsertDefinition("asyncAdd.yaml", yaml))
	wf, err := rt.LoadWorkflow(ctx, "asyncAdd.yaml")
	assert.NoError(t, err)

	_ = rt.Start(ctx)

	init := map[string]interface{}{
		"numbers": []interface{}{
			map[string]int{"a": 1, "b": 2},
			map[string]int{"a": 3, "b": 4},
		},
	}

	_, wait, err := rt.StartProcess(ctx, wf, init)
	assert.NoError(t, err)

	out, err := wait(ctx, 2*time.Second)
	assert.NoError(t, err)

	if parentOut, ok := out.Output["parent"].([]interface{}); ok {
		assert.ElementsMatch(t, []interface{}{3, 7}, parentOut)
	} else {
		t.Fatalf("parent output missing or wrong type: %v", out.Output)
	}
}
