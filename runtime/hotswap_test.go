package fluxor_test

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/viant/fluxor"
	"testing"
)

// TestRuntimeHotSwap verifies the Runtime.UpsertDefinition and Runtime.RefreshWorkflow
// helpers that support hot-swapping workflow definitions at runtime. The test
// runs through a sequence of scenarios using a data-driven approach.
func TestRuntimeHotSwap(t *testing.T) {
	type testCase struct {
		description string
		location    string
		data        []byte
		expected    string // expected workflow name after the operation
		refresh     bool   // whether to invoke RefreshWorkflow instead of Upsert
	}

	const location = "test.yaml"

	yamlV1 := []byte(`
name: test1
pipeline:
  kind: Sequence
  tasks: []
`)

	yamlV2 := []byte(`
name: test2
pipeline:
  kind: Sequence
  tasks: []
`)

	cases := []testCase{
		{
			description: "initial upsert", location: location, data: yamlV1, expected: "test1",
		},
		{
			description: "override with new definition", location: location, data: yamlV2, expected: "test2",
		},
		{
			description: "refresh (invalidate cache)", location: location, refresh: true, expected: "test2", // load should trigger lazy reload retaining last stored definition
		},
	}

	// Create a baseline Fluxor service (in-memory – no meta service interaction
	// as we work purely with UpsertDefinition).
	srv := fluxor.New()
	rt := srv.Runtime()
	ctx := context.Background()

	for _, tc := range cases {
		if tc.refresh {
			assert.Nil(t, rt.RefreshWorkflow(tc.location), tc.description)
		} else {
			assert.Nil(t, rt.UpsertDefinition(tc.location, tc.data), tc.description)
		}

		wf, err := rt.LoadWorkflow(ctx, tc.location)
		if !assert.Nil(t, err, tc.description) {
			continue
		}
		assert.EqualValues(t, tc.expected, wf.Name, tc.description)
	}
}
