package input

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestService_AskAndForm(t *testing.T) {
	type testCase struct {
		name     string
		method   string
		input    interface{}
		userIO   string // simulated user keystrokes
		expected interface{}
	}

	cases := []testCase{
		{
			name:   "ask free-form",
			method: "ask",
			input: &AskInput{
				Message: "Your name?",
				Default: "anon",
			},
			userIO:   "Bob\n",
			expected: &AskOutput{Text: "Bob"},
		},
		{
			name:   "ask default when empty",
			method: "ask",
			input: &AskInput{
				Message: "Your city?",
				Default: "NYC",
			},
			userIO:   "\n", // user hits Enter – choose default
			expected: &AskOutput{Text: "NYC"},
		},
		{
			name:   "form free + radio (index)",
			method: "form",
			input: &FormInput{
				Message: "Account setup",
				Fields: []Field{
					{Label: "username", Name: "user"},
					{Label: "role", Name: "role", Options: []string{"admin", "viewer"}},
				},
			},
			userIO:   "alice\n1\n",
			expected: &FormOutput{Values: map[string]string{"user": "alice", "role": "admin"}},
		},
		{
			name:   "form radio by value + default",
			method: "form",
			input: &FormInput{
				Fields: []Field{{Label: "theme", Options: []string{"dark", "light"}, Default: "light"}},
			},
			userIO:   "\n", // choose default
			expected: &FormOutput{Values: map[string]string{"theme": "light"}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inR := strings.NewReader(tc.userIO)
			outW := new(strings.Builder)

			svc := NewWithIO(inR, outW)

			exec, err := svc.Method(tc.method)
			if !assert.NoError(t, err) {
				return
			}

			ctx := context.Background()
			var out interface{}
			switch tc.method {
			case "ask":
				out = &AskOutput{}
			case "form":
				out = &FormOutput{}
			}

			if !assert.NoError(t, exec(ctx, tc.input, out)) {
				return
			}
			assert.EqualValues(t, tc.expected, out)
		})
	}
}
