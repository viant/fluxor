package expander

import (
	"github.com/viant/fluxor/service/action/system/exec"
	"testing"
)

func TestExpandExecOutputStruct(t *testing.T) {
	data := &exec.Output{Stdout: "running 0"}
	state := map[string]interface{}{"exec": data}
	got, _ := Expand("${exec.Stdout}", state)
	if got != "running 0" {
		t.Errorf("expected running 0, got %v", got)
	}
}
