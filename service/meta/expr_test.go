package meta

import (
	"os"
	"testing"
)

func TestExpandEnvExpr(t *testing.T) {
	tests := []struct {
		name     string
		env      map[string]string // env vars to set for this test
		input    string
		expected string
	}{
		{
			name:     "no expressions",
			env:      nil,
			input:    "just a plain string",
			expected: "just a plain string",
		},
		{
			name: "single expression",
			env: map[string]string{
				"FOO": "bar",
			},
			input:    "value is ${env.FOO}",
			expected: "value is bar",
		},
		{
			name: "multiple expressions",
			env: map[string]string{
				"A": "1",
				"B": "2",
			},
			input:    "${env.A}-${env.B}-${env.A}",
			expected: "1-2-1",
		},
		{
			name:     "unset variable becomes empty",
			env:      nil,
			input:    "unset=${env.NOTSET}-end",
			expected: "unset=-end",
		},
		{
			name:     "malformed missing closing brace",
			env:      map[string]string{"X": "x"},
			input:    "start ${env.X and ${env.Y} end",
			expected: "start ${env.X and  end",
		},
		{
			name:     "prefix only no key",
			env:      nil,
			input:    "oops ${env.} done",
			expected: "oops  done",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// set up environment
			// clear any previously set TEST_* variables
			// then set the ones for this test
			for _, kv := range []string{"FOO", "A", "B", "X", "Y", "NOTSET"} {
				os.Unsetenv(kv)
			}
			for k, v := range tc.env {
				os.Setenv(k, v)
			}

			got := expandEnvExpr(tc.input)
			if got != tc.expected {
				t.Errorf("expandEnvExpr(%q) = %q; want %q", tc.input, got, tc.expected)
			}
		})
	}
}
