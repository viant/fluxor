package fluxor

import "fmt"

// Config is a serialisable representation of the engine configuration. It can
// be populated from JSON, YAML, TOML, environment variables, etc. The
// zero-value is useful – all nested fields inherit their package defaults.

type Config struct {
	Processor ProcessorConfig `json:"processor" yaml:"processor"`
	// In the future add Executor, Messaging, Approval …
}

type ProcessorConfig struct {
	WorkerCount int `json:"workers" yaml:"workers"`
}

// DefaultConfig returns a Config populated with exactly the same default
// values that were previously hard-coded in the constructors. Callers may
// modify the returned struct before passing it to NewFromConfig.
func DefaultConfig() *Config {
	return &Config{
		Processor: ProcessorConfig{
			WorkerCount: 100, // matches hard-coded value in Service.init previously
		},
	}
}

// Validate returns aggregated error describing invalid settings or nil.
func (c *Config) Validate() error {
	if c == nil {
		return nil
	}
	if c.Processor.WorkerCount <= 0 {
		return fmt.Errorf("processor.workerCount must be > 0")
	}
	return nil
}
