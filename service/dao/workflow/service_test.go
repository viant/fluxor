package workflow

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/afs"
	_ "github.com/viant/afs/embed"

	"github.com/viant/fluxor/model"
	"github.com/viant/fluxor/service/meta"
	"testing"
)

// testFS holds our test YAML files
//
//go:embed testdata/*
var testFS embed.FS

// TestService_Load tests the workflow loading functionality
func TestService_Load(t *testing.T) {
	// Set up memory file system
	ctx := context.Background()

	// Test cases
	testCases := []struct {
		name           string
		url            string
		expectedErr    bool
		expectJSON     string
		expect         model.Workflow
		adjustExpected func(*model.Workflow)
	}{
		{
			name: "Valid workflow",
			url:  "flow.yaml",
			expectJSON: `{
  "source": {
    "url": "flow.yaml"
  },
  "name": "flow",
  "Imports": null,
  "init": [
    {
      "name": "x",
      "value": 0.0
    }
  ],
  "pipeline": {
    "id": "flow",
    "tasks": [
      {
        "id": "flow/task1",
        "name": "task1",
        "namespace": "task1",
        "action": {
          "service": "system/executor"
        }
      },
      {
        "id": "flow/task2",
        "name": "task2",
        "namespace": "task2",
        "tasks": [
          {
            "id": "flow/task2/subTask",
            "name": "subTask",
            "namespace": "subTask",
            "action": {
              "service": "printer",
              "method": "print",
              "input": {
                "message": "$task1.Output"
              }
            },
            "dependsOn": [
              "taskZ"
            ]
          }
        ]
      }
    ]
  },
  "dependencies": {
    "taskZ": {
      "id": "taskZ",
      "name": "taskZ",
      "namespace": "taskZ",
      "action": {
        "service": "printer",
        "method": "print",
        "input": {
          "message": "taskZ"
        }
      }
    }
  }
}`,
		},
	}

	// Run test cases
	for _, tc := range testCases {

		service := New(WithMetaService(meta.New(afs.New(), "embed:///testdata", &testFS)))

		t.Run(tc.name, func(t *testing.T) {
			actual, err := service.Load(ctx, tc.url)

			if tc.expectedErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, actual)

			expect := tc.expect
			if tc.expectJSON != "" {
				err = json.Unmarshal([]byte(tc.expectJSON), &expect)
				assert.Nil(t, err)
			}
			if !assert.EqualValues(t, expect, *actual, tc.name) {
				actual, _ := json.MarshalIndent(actual, "", "  ")
				fmt.Println(string(actual))
			}
		})
	}
}
