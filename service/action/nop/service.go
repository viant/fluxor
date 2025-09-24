package nop

import (
	"context"
	"github.com/viant/fluxor/model/types"
	"reflect"
)

const name = "nop"

// Service extracts structured information from LLM responses
type Service struct{}

type Input struct{}

// Output represents output from extraction
type Output struct {
}

// New creates a new extractor service
func New() *Service {
	return &Service{}
}

// Name returns the service name
func (s *Service) Name() string {
	return name
}

// Methods returns the service methods
func (s *Service) Methods() types.Signatures {
	return []types.Signature{
		{
			Name:        "nop",
			Description: "Performs no operation and returns immediately.",
			Internal:    true,
			Input:       reflect.TypeOf(&Input{}),
			Output:      reflect.TypeOf(&Output{}),
		},
	}
}

// Method returns the specified method
func (s *Service) Method(name string) (types.Executable, error) {
	return s.nop, nil
}

// does nothing
func (s *Service) nop(ctx context.Context, in, out interface{}) error {
	return nil
}
