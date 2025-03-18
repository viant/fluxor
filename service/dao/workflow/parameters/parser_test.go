package parameters

import (
	"github.com/stretchr/testify/assert"
	bstate "github.com/viant/bindly/state"
	"github.com/viant/fluxor/model/state"
	"testing"
)

func TestParse(t *testing.T) {
	testCases := []struct {
		description string
		input       string
		expected    *state.Parameter
		shouldError bool
	}{
		{
			description: "basic variable with type and kind/location",
			input:       "myParam[com.example.Type](kind/location)",
			expected: &state.Parameter{
				Name:     "myParam",
				DataType: "com.example.Type",
				Location: &bstate.Location{
					Kind: "kind",
					In:   "location",
				},
			},
			shouldError: false,
		},
		{
			description: "variable with only kind",
			input:       "userID[java.lang.String](id)",
			expected: &state.Parameter{
				Name:     "userID",
				DataType: "java.lang.String",
				Location: &bstate.Location{
					Kind: "id",
				},
			},
			shouldError: false,
		},

		{
			description: "variable with only kind",
			input:       "Projects[repository.Project]()",
			expected: &state.Parameter{
				Name:     "Projects",
				DataType: "repository.Project",
				Location: &bstate.Location{
					Kind: "",
				},
			},
			shouldError: false,
		},

		{
			description: "variable with complex type",
			input:       "items[java.util.List[java.lang.String]](collection/memory)",
			expected: &state.Parameter{
				Name:     "items",
				DataType: "java.util.List[java.lang.String]",
				Location: &bstate.Location{
					Kind: "collection",
					In:   "memory",
				},
			},
			shouldError: false,
		},
		{
			description: "variable with URI location",
			input:       "config[com.example.Config](resource/file:///etc/config.json)",
			expected: &state.Parameter{
				Name:     "config",
				DataType: "com.example.Config",
				Location: &bstate.Location{
					Kind: "resource",
					In:   "file:///etc/config.json",
				},
			},
			shouldError: false,
		},
		{
			description: "invalid variable - missing closing angle bracket",
			input:       "myParam[com.example.Type(kind/location)",
			shouldError: true,
		},
		{
			description: "invalid variable - missing opening parenthesis",
			input:       "myParam[com.example.Type]kind/location)",
			shouldError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			result, err := Parse([]byte(tc.input))

			if tc.shouldError {
				assert.Error(t, err)
			} else {
				assert.EqualValues(t, tc.expected, result)
				assert.NoError(t, err)
			}
		})
	}
}
