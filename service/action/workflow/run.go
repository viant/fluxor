package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/fluxor/model"
	"github.com/viant/fluxor/model/execution"
	"github.com/viant/fluxor/model/types"
)

type RunInput struct {
	ParentLocation string                 `json:"parentLocation,omitempty"`
	Location       string                 `json:"location,omitempty"`
	Tasks          []string               `json:"tasks,omitempty"`
	Context        map[string]interface{} `json:"parameters,omitempty"`
	Workflow       *model.Workflow        `json:"workflow,omitempty"`
	IgnoreError    bool                   `json:"throwError,omitempty"`
	Async          bool                   `json:"wait,omitempty"`
	WaitTimeInSec  int                    `json:"WaitTimeInSec,omitempty"`
}

type RunOutput struct {
	ProcessID string
	Output    map[string]interface{}
	Errors    map[string]string
	State     string
}

func (i *RunInput) Init(ctx context.Context) {
	if url.IsRelative(i.Location) && i.ParentLocation != "" { //for relative location try resolve with parent
		parent, _ := url.Split(i.Location, file.Scheme)
		candidate := url.Join(parent, i.Location)
		fs := afs.New()
		if ok, _ := fs.Exists(ctx, candidate); ok {
			i.Location = candidate
		}
	}
	if i.WaitTimeInSec == 0 && !i.Async {
		i.WaitTimeInSec = 300 //5 min
	}
}

func (i *RunInput) Validate(ctx context.Context) error {
	if i.Workflow != nil {
		return nil
	}
	if i.Location == "" {
		return fmt.Errorf("location is required")
	}

	return nil
}

// print processes LLM responses to print structured data
func (s *Service) run(ctx context.Context, in, out interface{}) error {
	input, ok := in.(*RunInput)
	if !ok {
		return types.NewInvalidInputError(in)
	}

	input.Init(ctx)

	if err := input.Validate(ctx); err != nil {
		return err
	}

	if err := s.ensureWorkflow(ctx, input); err != nil {
		return err
	}

	process, err := s.processor.StartProcess(ctx, input.Workflow, input.Context, input.Tasks...)
	if err != nil {
		return err
	}
	output, ok := out.(*RunOutput)
	if !ok {
		return types.NewInvalidOutputError(out)
	}
	output.ProcessID = process.ID
	if !input.Async {
		waitInput := &WaitInput{
			ProcessID:    process.ID,
			TimeoutInSec: input.WaitTimeInSec,
		}
		waitOutput := &WaitOutput{}
		if err := s.wait(ctx, waitInput, waitOutput); err != nil {
			return err
		}

		if waitOutput.State == execution.StateFailed {
			if !input.IgnoreError {
				errorInfo, _ := json.Marshal(waitOutput.Errors)
				return fmt.Errorf("failed to run process %v, due to %s", process.ID, errorInfo)
			}
		}
		output.Output = waitOutput.Output
		output.Errors = waitOutput.Errors
		output.State = waitOutput.State
	}
	return nil
}
