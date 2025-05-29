package exec

// Command represents the result of executing a single command
type Command struct {
	Input  string `json:"input,omitempty"`  // The command that was executed
	Output string `json:"output,omitempty"` // Standard output from the command
	Stderr string `json:"stderr,omitempty"` // Standard error from the command
	Status int    `json:"status,omitempty"` // Exit code of the command
}

// Output represents the results of executing commands
type Output struct {
	Commands []*Command `json:"commands,omitempty"` // Results of individual commands
	Stdout   string     `json:"stdout,omitempty"`   // Combined standard output from all commands
	Stderr   string     `json:"stderr,omitempty"`   // Combined standard error from all commands
	Status   int        `json:"status,omitempty"`   // Exit code of the last command executed
}
