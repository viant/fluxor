package workflow

import (
	"context"
	"fmt"
	"github.com/viant/fluxor/model/types"
)

type StatusInput struct {
	ProcessID string `json:"processID,omitempty"`
}

func (i *StatusInput) Validate(ctx context.Context) error {
	if i.ProcessID == "" {
		return fmt.Errorf("processID is required")
	}
	return nil
}

type StatusOutput struct {
	State  string
	Output map[string]interface{}
}

// print processes LLM responses to print structured data
func (s *Service) status(ctx context.Context, in, out interface{}) error {
	input, ok := in.(*StatusInput)
	if !ok {
		return types.NewInvalidInputError(in)
	}

	if err := input.Validate(ctx); err != nil {
		return err
	}

	process, err := s.processDao.Load(ctx, input.ProcessID)
	if err != nil {
		return err
	}

	output, ok := in.(*StatusOutput)
	if !ok {
		return types.NewInvalidOutputError(out)
	}
	output.State = process.State
	output.Output = process.Session.State
	return nil
}
