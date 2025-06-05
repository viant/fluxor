package printer

import (
	"context"
	"fmt"
	"github.com/viant/fluxor/model/types"
	"reflect"
	"strings"
)

const name = "printer"

// Service extracts structured information from LLM responses
type Service struct{}

type Input struct {
	Message string
}

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
			Name:        "print",
			Description: "Prints the given message to standard output.",
			Input:       reflect.TypeOf(&Input{}),
			Output:      reflect.TypeOf(&Output{}),
		},
	}
}

// Method returns the specified method
func (s *Service) Method(name string) (types.Executable, error) {
	switch strings.ToLower(name) {
	case "print":
		return s.print, nil
	default:
		return nil, types.NewMethodNotFoundError(name)
	}
}

// print processes LLM responses to print structured data
func (s *Service) print(ctx context.Context, in, out interface{}) error {
	input, ok := in.(*Input)
	if !ok {
		return types.NewInvalidInputError(in)
	}
	fmt.Println(input.Message)
	return nil
}
