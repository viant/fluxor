package input

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"

	"github.com/viant/fluxor/model/types"
)

// Name of the service as used by workflows.
const Name = "input"

// Service implements types.Service and allows workflows to collect data from a user
// via standard input/output.  Two methods are exposed:
//
//	ask   – free-form text prompt (single field)
//	form  – multi-field prompt with optional predefined choices (radio-style)
//
// Tests can substitute Reader/Writer to avoid interactive TTY requirements.
type Service struct {
	in  io.Reader
	out io.Writer
}

// New returns a Service that reads from stdin and writes to stdout.
func New() *Service {
	return &Service{in: os.Stdin, out: os.Stdout}
}

// NewWithIO lets callers override the input/output streams (handy for tests).
func NewWithIO(in io.Reader, out io.Writer) *Service {
	if in == nil {
		in = os.Stdin
	}
	if out == nil {
		out = os.Stdout
	}
	return &Service{in: in, out: out}
}

// -------------------------------------------------------------------------
// Method: ask – free-form question
// -------------------------------------------------------------------------

type AskInput struct {
	Message string `json:"message,omitempty"` // prompt shown to the user
	Default string `json:"default,omitempty"` // fallback value if the user enters empty line
}

type AskOutput struct {
	Text string `json:"text,omitempty"`
}

func (s *Service) ask(ctx context.Context, in, out interface{}) error {
	input, ok := in.(*AskInput)
	if !ok {
		return types.NewInvalidInputError(in)
	}
	output, ok := out.(*AskOutput)
	if !ok {
		return types.NewInvalidOutputError(out)
	}

	reader := bufio.NewReader(s.in)

	// Ensure the prompt ends with a space for nicer UX.
	prompt := strings.TrimSpace(input.Message)
	if prompt == "" {
		prompt = "?"
	}
	if !strings.HasSuffix(prompt, " ") {
		prompt += " "
	}

	fmt.Fprint(s.out, prompt)

	response, err := reader.ReadString('\n')
	if err != nil && err != io.EOF {
		return err
	}
	response = strings.TrimSpace(response)
	if response == "" {
		response = input.Default
	}

	output.Text = response
	return nil
}

// -------------------------------------------------------------------------
// Method: form – multi-field questionaire
// -------------------------------------------------------------------------

// Field describes a single input element in a form.
type Field struct {
	// Label displayed to the user (required).
	Label string `json:"label,omitempty"`
	// Name used as the key in the resulting map (defaults to Label if empty).
	Name string `json:"name,omitempty"`
	// Options – if provided – make this a single-choice (radio) field.  A user
	// can answer with either a number (1-N) corresponding to the option index
	// or by typing the option value verbatim (case-insensitive).
	Options []string `json:"options,omitempty"`
	// Default value returned when the user enters empty line.
	Default string `json:"default,omitempty"`
}

type FormInput struct {
	Fields []Field `json:"fields,omitempty"`
	// Optional introductory message displayed before the form.
	Message string `json:"message,omitempty"`
}

type FormOutput struct {
	Values map[string]string `json:"values,omitempty"`
}

func (s *Service) form(ctx context.Context, in, out interface{}) error {
	input, ok := in.(*FormInput)
	if !ok {
		return types.NewInvalidInputError(in)
	}
	output, ok := out.(*FormOutput)
	if !ok {
		return types.NewInvalidOutputError(out)
	}

	if len(input.Fields) == 0 {
		// Nothing to ask – return empty map.
		output.Values = map[string]string{}
		return nil
	}

	reader := bufio.NewReader(s.in)
	if strings.TrimSpace(input.Message) != "" {
		fmt.Fprintf(s.out, "%s\n", strings.TrimSpace(input.Message))
	}

	output.Values = make(map[string]string, len(input.Fields))

	for _, field := range input.Fields {
		label := strings.TrimSpace(field.Label)
		if label == "" {
			label = "?"
		}

		name := field.Name
		if name == "" {
			name = label
		}

		var prompt strings.Builder
		prompt.WriteString(label)

		// Append options inline for radio fields.
		if len(field.Options) > 0 {
			for i, opt := range field.Options {
				if i == 0 {
					prompt.WriteString(" (")
				} else {
					prompt.WriteString(", ")
				}
				prompt.WriteString(fmt.Sprintf("%d:%s", i+1, opt))
			}
			prompt.WriteString(")")
		}
		prompt.WriteString(": ")

		fmt.Fprint(s.out, prompt.String())

		response, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			return err
		}
		response = strings.TrimSpace(response)
		if response == "" {
			response = field.Default
		}

		// If field has predefined options, translate numerical selection.
		if len(field.Options) > 0 {
			if idx, ok := parseIndex(response, len(field.Options)); ok {
				response = field.Options[idx]
			}
		}

		output.Values[name] = response
	}

	return nil
}

func parseIndex(s string, n int) (int, bool) {
	if n == 0 {
		return 0, false
	}
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false
	}
	// Accept 1-based integer index.
	var idx int
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, false
		}
		idx = idx*10 + int(r-'0')
	}
	idx-- // convert to 0-based
	if idx < 0 || idx >= n {
		return 0, false
	}
	return idx, true
}

// -------------------------------------------------------------------------
// Boilerplate – types.Service api
// -------------------------------------------------------------------------

func (s *Service) Name() string { return Name }

func (s *Service) Methods() types.Signatures {
	return []types.Signature{
		{
			Name:        "ask",
			Description: "Prompts the user for free-form input and returns the response.",
			Input:       reflect.TypeOf(&AskInput{}),
			Output:      reflect.TypeOf(&AskOutput{}),
		},
		{
			Name:        "form",
			Description: "Prompts the user with multiple questions. Supports optional single-choice (radio) fields.",
			Input:       reflect.TypeOf(&FormInput{}),
			Output:      reflect.TypeOf(&FormOutput{}),
		},
	}
}

func (s *Service) Method(name string) (types.Executable, error) {
	switch strings.ToLower(name) {
	case "ask":
		return s.ask, nil
	case "form":
		return s.form, nil
	default:
		return nil, types.NewMethodNotFoundError(name)
	}
}
