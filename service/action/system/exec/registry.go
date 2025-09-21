package exec

import (
	"context"
	"reflect"
	"strings"

	"github.com/viant/fluxor/model/types"
)

const Name = "system/exec"

func (s *Service) Name() string {
	return Name
}

func (s *Service) Methods() types.Signatures {
	return []types.Signature{
		{
			Name: "execute",
			Description: `Executes one or more shell commands on the local host.

• Always set "directory" to the workdir for the agent session,
  even if commands use absolute paths or include an explicit 'cd'.
  This ensures predictable and consistent execution context.

• Each execution is ephemeral and starts in a fresh environment,
  so context (like current directory) must be explicitly re-established.

Examples:
- Single command:
  "commands": ["ls -la /tmp"],
  "directory": "<workdir>"

- Multiple commands:
  "commands": [
     "cd /var/log",
     "grep -i error *.log > /tmp/errors.txt"
  ],
  "directory": "<workdir>"

Usage constraints for file reading commands (cat, sed):
` + "- Single-read rule: When reading an entire small file (≤10 kB), call this tool ONCE using `cat <path>` or `sed -n '1,$p' <path>`. Do not page with multiple `sed -n 'start,endp'`." + `
- Redundancy rule: Do not issue immediately consecutive commands that return overlapping lines from the same file. Collapse into one call.
` + "- Chunking rule: Only use paged `sed` ranges for large files AND provide a one-sentence justification before emitting the calls." + `
Bad → Good (for customer.dql file with size less or equal 10kB):
Bad:  sed -n '1,120p' dql/customer/customer.dql
      sed -n '1,160p' dql/customer/customer.dql
Good: cat dql/customer/customer.dql

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
