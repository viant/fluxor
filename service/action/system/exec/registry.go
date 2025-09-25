package exec

import (
	"context"
	_ "embed"
	"reflect"
	"strings"

	"github.com/viant/fluxor/model/types"
)

const Name = "system/exec"

func (s *Service) Name() string {
	return Name
}

//go:embed exec_spec.md
var description string

func (s *Service) Methods() types.Signatures {
	return []types.Signature{
		{
			Name:        "execute",
			Description: description,
			Input:       reflect.TypeOf(&Input{}),
			Output:      reflect.TypeOf(&Output{}),
		}}
}

func (s *Service) execute(context context.Context, in, out interface{}) error {
	input, ok := in.(*Input)
	if !ok {
		return types.NewInvalidInputError(in)
	}
	output, ok := out.(*Output)
	if !ok {
		return types.NewInvalidOutputError(out)
	}
	return s.Execute(context, input, output)
}

// Method returns method by Name
func (s *Service) Method(name string) (types.Executable, error) {
	switch strings.ToLower(name) {
	case "execute":
		return s.execute, nil
	default:
		return nil, types.NewMethodNotFoundError(name)
	}
}
