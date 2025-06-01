package workflow

import (
	"context"
	"fmt"
	"github.com/viant/fluxor/model/types"
	"github.com/viant/fluxor/runtime/execution"
	"time"
)

type WaitInput struct {
	ProcessID         string `json:"processID,omitempty"`
	TimeoutInMs       int    `json:"timeoutSec,omitempty"`
	PoolFrequencyInMs int    `json:"poolTimeMs,omitempty"`
}

func (i *WaitInput) Init(ctx context.Context) {
	if i.PoolFrequencyInMs == 0 {
		i.PoolFrequencyInMs = 200
	}
	if i.TimeoutInMs == 0 {
		i.TimeoutInMs = 300000 //5 min
	}
}

func (i *WaitInput) Validate(ctx context.Context) error {
	if i.ProcessID == "" {
		return fmt.Errorf("processID is required")
	}
	return nil
}

// WaitOutput represents a wait output
type WaitOutput execution.ProcessOutput

// WaitForProcess waits for a process to complete
func (s *Service) WaitForProcess(ctx context.Context, id string, timeoutMs int) (*WaitOutput, error) {
	input := &WaitInput{ProcessID: id}
	input.TimeoutInMs = timeoutMs
	input.Init(ctx)
	output := &WaitOutput{}
	return output, s.wait(ctx, input, output)
}

// print processes LLM responses to print structured data
func (s *Service) wait(ctx context.Context, in, out interface{}) error {
	input, ok := in.(*WaitInput)
	if !ok {
		return types.NewInvalidInputError(in)
	}

	if err := input.Validate(ctx); err != nil {
		return err
	}

	output, ok := out.(*WaitOutput)
	if !ok {
		return types.NewInvalidOutputError(out)
	}

	poolFrequency := time.Millisecond * time.Duration(input.PoolFrequencyInMs)
	var expiry time.Time
	if input.TimeoutInMs > 0 {
		expiry = time.Now().Add(time.Millisecond * time.Duration(input.TimeoutInMs))
	}

	//Always populate process ID so that caller can correlate the result even
	//when the workflow finishes with an error or times-out.
	output.ProcessID = input.ProcessID

outer:
	for {
		process, err := s.processDao.Load(ctx, input.ProcessID)
		if err != nil {
			return err
		}
		// Finished only when allocator sets final state.
		if process.State == execution.StateCompleted || process.State == execution.StateFailed {
			break outer // done
		}

		if !expiry.IsZero() && time.Now().After(expiry) {
			output.Timeout = true
			break outer // timeout reached
		}
		time.Sleep(poolFrequency)

	}
	process, err := s.processDao.Load(ctx, input.ProcessID)
	if err != nil {
		return err
	}

	output.State = process.State
	output.Output = process.Session.State
	output.Errors = process.Errors
	finishedAt := process.FinishedAt
	if finishedAt == nil {
		ts := time.Now()
		finishedAt = &ts
	}
	output.TimeTaken = finishedAt.Sub(process.CreatedAt)
	return nil
}
