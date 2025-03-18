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
  "init": [
    {
      "name": "i",
      "value": 0
    }
  ],
  "pipeline": {
    "tasks": [
      {
        "id": "task1",
        "init": [
          {
            "name": "i",
            "value": "${ i+1 }"
          }
        ],
        "action": {
          "service": "nop"
        }
      },
      {
        "id": "task2",
        "tasks": [
          {
            "id": "subTask",
            "action": {
              "service": "printer:print",
              "input": {
                "message": "${{ task.i }}"
              }
            }
          },
          {
            "id": "task3",
            "transitions": [
              {
                "when": "${i \u003c 5}",
                "goto": "task1"
              }
            ]
          }
        ]
      }
    ]
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
