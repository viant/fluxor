package executor

import (
	"github.com/viant/fluxor/service/action/connector"
)

// Input represents system executor configuration
type Input struct {
	Host         *connector.Host   `description:"host to execute command on" required:"true" json:"host,omitempty"`                                                //host to execute command on
	Directory    string            `description:"directory where this command should start - if does not exists there is no exception" json:"directory,omitempty"` //directory where command should run
	Env          map[string]string `description:"environment variables to be set before command runs" json:"env,omitempty"`                                        //environment variables to be set before command runs
	Commands     []string          `json:"commands,omitempty" description:"commands to execute on the target system"`                                              //commands to run
	AbortOnError *bool             `description:"check after command execution if status is <> 0, then throws error" json:"abortOnError,omitempty"`                //whether to abort on error
}

func (i *Input) Init() {
	if i.Host == nil {
		i.Host = &connector.Host{}
	}
	if i.Host.URL == "" {
		i.Host.URL = "bash://localhost/"
	}
}
