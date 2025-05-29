package exec

import (
	"context"
	"github.com/viant/fluxor/model/types"
	"reflect"
	"strings"
)

const Name = "system/exec"

func (s *Service) Name() string {
	return Name
}

func (s *Service) Methods() types.Signatures {
	return []types.Signature{
		{
			Name:        "execute",
			Description: "Executes a shell command on a remote host, or locally if no remote target is specified.",
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
