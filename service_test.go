package fluxor_test

import (
	"context"
	"embed"
	"fmt"
	"github.com/stretchr/testify/assert"
	_ "github.com/viant/afs/embed"
	"github.com/viant/fluxor"
	"github.com/viant/fluxor/runtime/execution"
	"testing"
	"time"
)

//go:embed testdata/*
var embedFS embed.FS

func TestService(t *testing.T) {
	srv := fluxor.New(
		fluxor.WithMetaFsOptions(&embedFS),
		fluxor.WithMetaBaseURL("embed:///testdata"),
		fluxor.WithTracing("fluxor", "0.0.1", "span.txt"),
	)

	runtime := srv.Runtime()
	ctx := context.Background()
	workflow, err := runtime.LoadWorkflow(ctx, "parent.yaml")
	if !assert.Nil(t, err) {
		return
	}
	assert.NotNil(t, workflow)
	_ = runtime.Start(ctx)
	process, wait, err := runtime.StartProcess(ctx, workflow, map[string]interface{}{})
	if !assert.Nil(t, err) {
		return
	}
	assert.NotNil(t, process)

	output, err := wait(ctx, time.Hour)
	fmt.Println(output, err)

}

// TestTemplate verifies the template feature by printing each order
func TestTemplate(t *testing.T) {
	// set up Fluxor service with embedded testdata
	srv := fluxor.New(
		fluxor.WithMetaFsOptions(&embedFS),
		fluxor.WithMetaBaseURL("embed:///testdata"),
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
	// Process should complete without errors
	assert.EqualValues(t, execution.StateCompleted, output.State)
}
