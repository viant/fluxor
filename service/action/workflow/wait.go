package workflow

import (
	"context"
	"fmt"
	"github.com/viant/fluxor/model/execution"
	"github.com/viant/fluxor/model/types"
	"time"
)

type WaitInput struct {
	ProcessID         string `json:"processID,omitempty"`
	TimeoutInSec      int    `json:"timeoutSec,omitempty"`
	PoolFrequencyInMs int    `json:"poolTimeMs,omitempty"`
}

func (i *WaitInput) Init(ctx context.Context) {
	if i.PoolFrequencyInMs == 0 {
		i.PoolFrequencyInMs = 200
	}
	if i.TimeoutInSec == 0 {
		i.TimeoutInSec = 300 //5 min
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
	input.TimeoutInSec = timeoutMs
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

	output, ok := in.(*WaitOutput)
	if !ok {
		return types.NewInvalidInputError(in)
	}

	poolFrequency := time.Millisecond * time.Duration(input.PoolFrequencyInMs)
	expiry := time.Now().Add(time.Second * time.Duration(input.TimeoutInSec))
	started := time.Now()

outer:
	for {
		process, err := s.processDao.Load(ctx, input.ProcessID)
		if err != nil {
			return err
		}
		switch process.State {
		case execution.StateCompleted, execution.StateFailed:
			break outer
		}
		if expiry.After(started) {
			output.Timeout = true
			break outer
		}
		time.Sleep(poolFrequency)

	}
	process, err := s.processDao.Load(ctx, input.ProcessID)
	if err != nil {
		return err
	}
	output.State = process.State
	output.Output = process.Session.State
	finishedAt := process.FinishedAt
	if finishedAt == nil {
		ts := time.Now()
		finishedAt = &ts
	}
	output.TimeTaken = finishedAt.Sub(process.CreatedAt)
	return nil
}
