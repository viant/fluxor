package exec

import (
	"github.com/viant/fluxor/service/action/system"
)

// Input represents system executor configuration
type Input struct {
	Host         *system.Host      `json:"host,omitempty" description:"host to execute command on"  internal:"true" `        //host to execute command on
	Workdir      string            `json:"workdir" required:"true" description:"directory where commands should start"`      //directory where command should run  - if does not exists there is no exception
	Env          map[string]string `json:"env,omitempty" description:"environment variables to be set before command runs" ` //environment variables to be set before command runs
	Commands     []string          `json:"commands,omitempty" description:"commands to execute on the target system"`        //commands to run
	TimeoutMs    int               `json:"timeoutMs,omitempty" yaml:"timeoutMs,omitempty" description:"max wiat time before timing out command"`
	AbortOnError *bool             `json:"abortOnError,omitempty" description:"check after command execution if status is <> 0, then throws error" ` //whether to abort on error
}

func (i *Input) Init() {
	if i.Host == nil {
		i.Host = &system.Host{}
	}
	if i.Host.URL == "" {
		i.Host.URL = "bash://localhost/"
	}
}
