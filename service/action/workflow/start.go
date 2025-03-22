package workflow

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/fluxor/model"
	"github.com/viant/fluxor/model/types"
)

type StartInput struct {
	ParentLocation string                 `json:"parentLocation,omitempty"`
	Location       string                 `json:"location,omitempty"`
	Tasks          []string               `json:"tasks,omitempty"`
	Context        map[string]interface{} `json:"parameters,omitempty"`
	Workflow       *model.Workflow        `json:"workflow,omitempty"`
	Wait           bool                   `json:"wait,omitempty"`
	WaitTimeInSec  int                    `json:"WaitTimeInSec,omitempty"`
}

type StartOutput struct {
	ProcessID string
	Output    map[string]interface{}
	State     string
}

func (i *StartInput) Init(ctx context.Context) {
	if url.IsRelative(i.Location) && i.ParentLocation != "" { //for relative location try resolve with parent
		parent, _ := url.Split(i.Location, file.Scheme)
		candidate := url.Join(parent, i.Location)
		fs := afs.New()
		if ok, _ := fs.Exists(ctx, candidate); ok {
			i.Location = candidate
		}
	}
}

func (i *StartInput) Validate(ctx context.Context) error {
	if i.Workflow != nil {
		return nil
	}
	if i.Location == "" {
		return fmt.Errorf("location is required")
	}

	return nil
}

// print processes LLM responses to print structured data
func (s *Service) start(ctx context.Context, in, out interface{}) error {
	input, ok := in.(*StartInput)
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

	process, err := s.processor.StartProcess(ctx, input.Workflow, input.Tasks, input.Context)
	if err != nil {
		return err
	}
	output, ok := in.(*StartOutput)
	if !ok {
		return types.NewInvalidInputError(in)
	}
	output.ProcessID = process.ID
	if input.Wait {
		waitInput := &WaitInput{
			ProcessID:    process.ID,
			TimeoutInSec: input.WaitTimeInSec,
		}
		waitOutput := &WaitOutput{}
		if err := s.wait(ctx, waitInput, waitOutput); err != nil {
			return err
		}
		output.Output = waitOutput.Output
		output.State = waitOutput.State
	}
	return nil
}
