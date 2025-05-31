package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/fluxor/runtime/execution"

	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/fluxor/model"
	"github.com/viant/fluxor/model/types"
	"github.com/viant/fluxor/tracing"
)

type RunInput struct {
	ParentLocation string                 `json:"parentLocation,omitempty"`
	Location       string                 `json:"location,omitempty"`
	Tasks          []string               `json:"tasks,omitempty"`
	Context        map[string]interface{} `json:"parameters,omitempty"`
	Workflow       *model.Workflow        `json:"workflow,omitempty"`
	Source         []byte                 `json:"source,omitempty"`
	IgnoreError    bool                   `json:"throwError,omitempty"`
	Async          bool                   `json:"wait,omitempty"`
	WaitTimeInMs   int                    `json:"WaitTimeInMs,omitempty"`
}

type RunOutput struct {
	ProcessID string
	Output    map[string]interface{}
	Errors    map[string]string
	State     string
	Trace     *tracing.Trace `json:"trace,omitempty"`
}

func (i *RunInput) Init(ctx context.Context) {
	if i.Location == "" {
		return
	}
	if url.IsRelative(i.Location) && i.ParentLocation != "" { //for relative location try resolve with parent
		parent, _ := url.Split(i.Location, file.Scheme)
		candidate := url.Join(parent, i.Location)
		fs := afs.New()
		if ok, _ := fs.Exists(ctx, candidate); ok {
			i.Location = candidate
		}
	}
	if i.WaitTimeInMs == 0 && !i.Async {
		i.WaitTimeInMs = 300000 //5 min
	}
}

func (i *RunInput) Validate(ctx context.Context) error {
	if i.Workflow != nil {
		return nil
	}
	if i.Location == "" && len(i.Source) == 0 {
		return fmt.Errorf("location is required")
	}
	return nil
}

// print processes LLM responses to print structured data
// run executes a workflow and captures tracing information
func (s *Service) run(ctx context.Context, in, out interface{}) (err error) {
	input, ok := in.(*RunInput)
	if !ok {
		return types.NewInvalidInputError(in)
	}
	// start tracing for workflow run
	trace := tracing.NewTrace("fluxor", "")
	ctx = tracing.WithTrace(ctx, trace)
	ctx, rootSpan := tracing.StartSpan(ctx, fmt.Sprintf("workflow:run %s", input.Location), "SERVER")
	defer tracing.EndSpan(rootSpan, err)

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
	process.Session.RegisterListeners(s.processor.Listeners()...)
	output, ok := out.(*RunOutput)
	if !ok {
		return types.NewInvalidOutputError(out)
	}
	output.ProcessID = process.ID
	if !input.Async {
		waitInput := &WaitInput{
			ProcessID:   process.ID,
			TimeoutInMs: input.WaitTimeInMs,
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
	// attach trace to output
	output.Trace = trace
	return nil
}
