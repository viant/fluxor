package fluxor_test

import (
	"context"
	"embed"
	"github.com/stretchr/testify/assert"
	_ "github.com/viant/afs/embed"
	"github.com/viant/fluxor"
	"testing"
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
	assert.Nil(t, err)
	assert.NotNil(t, workflow)

}
