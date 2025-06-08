package workflow

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

// TestParseSequencePipeline verifies that a pipeline defined as a YAML sequence
// of task definitions is successfully decoded by the workflow DAO service.  The
// format mirrors the example shown in the project README.
func TestParseSequencePipeline(t *testing.T) {
	yamlText := `
pipeline:
  - id: list
    service: system/storage
    action: list
    with:
      Location: "file://."
  - id: show
    service: printer
    action: print
    with:
      message: ${ $.list.entries }
`

	svc := New()
	wf, err := svc.DecodeYAML([]byte(yamlText))
	if err != nil {
		t.Fatalf("failed to decode sequence pipeline: %v", err)
	}

	// The root task created by parseRootTask is an anonymous holder.  The real
	// user-defined tasks are in wf.Pipeline.Tasks slice.
	assert.EqualValues(t, 2, len(wf.Pipeline.Tasks))

	listTask := wf.Pipeline.Tasks[0]
	assert.EqualValues(t, wf.Name+"/list", listTask.ID)
	assert.EqualValues(t, "system/storage", listTask.Action.Service)
	assert.EqualValues(t, "list", listTask.Action.Method)

	showTask := wf.Pipeline.Tasks[1]
	assert.EqualValues(t, wf.Name+"/show", showTask.ID)
	assert.EqualValues(t, "printer", showTask.Action.Service)
	assert.EqualValues(t, "print", showTask.Action.Method)

	// Ensure workflow validation passes.
	assert.Empty(t, wf.Validate())
}

// TestParseSequencePipelineCompact verifies that the DAO can decode an ordered
// sequence where each item is a single-entry mapping whose key is used as the
// task identifier.
func TestParseSequencePipelineCompact(t *testing.T) {
	yamlText := `
pipeline:
  - list:
      service: system/storage
      action: list
      with:
        Location: "file://."
  - show:
      service: printer
      action: print
      with:
        message: ${ $.list.entries }
`

	svc := New()
	wf, err := svc.DecodeYAML([]byte(yamlText))
	if err != nil {
		t.Fatalf("failed to decode compact sequence pipeline: %v", err)
	}

	assert.EqualValues(t, 2, len(wf.Pipeline.Tasks))
	assert.EqualValues(t, wf.Name+"/list", wf.Pipeline.Tasks[0].ID)
	assert.EqualValues(t, wf.Name+"/show", wf.Pipeline.Tasks[1].ID)
}
