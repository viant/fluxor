package fluxor_test

import (
	"context"
	"embed"
	"fmt"
	"github.com/stretchr/testify/assert"
	_ "github.com/viant/afs/embed"
	"github.com/viant/fluxor"
	"testing"
	"time"
)

//go:embed testdata/*
var embedFS embed.FS

func TestService(t *testing.T) {
	srv := fluxor.New(
		fluxor.WithMetaFsOptions(&embedFS),
		fluxor.WithMetaBaseURL("embed:///testdata"),
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
