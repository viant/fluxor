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
			Name: "execute",
			Description: `Executes one or more shell commands local host.
When using system_exec-execute, supply "directory" or start command with command:["cd <workdir>", "...."]
Each execution is ephemeral and starts in a fresh environment, so context must be explicitly re-established on every call.
Examples
• Run a single command
  "commands": ["ls -la /tmp"]
• Execute several commands sequentially
  "commands": [
     "cd /var/log",
     "grep -i error *.log > /tmp/errors.txt"
  ]
` + "Note:\nDO NOT USE `ls -R`, `find`, `grep`  in any form as this will lead to a very slow response, and exceeding context window\nUse \\`rg\\` and \\`rg --files\\`.\nwhen using rg always use --files or --search-path\n",
			Input:  reflect.TypeOf(&Input{}),
			Output: reflect.TypeOf(&Output{}),
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
