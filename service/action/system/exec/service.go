package exec

import (
	"context"
	"fmt"
	"time"

	"github.com/viant/afs/url"
	"github.com/viant/fluxor/service/action/system"
	"github.com/viant/gosh"
	"github.com/viant/gosh/runner"
	"github.com/viant/gosh/runner/local"
	rssh "github.com/viant/gosh/runner/ssh"
	"golang.org/x/crypto/ssh"

	"strings"

	"github.com/viant/scy/cred/secret"
)

const timeoutCode = -101

// Service struct for executing terminal commands
type Service struct{}

type sessionInfo struct {
	id      string
	service *gosh.Service
	close   func()
}

// New creates a new Service instance
func New() *Service {
	return &Service{}
}

// Execute executes terminal commands on the target system
func (s *Service) Execute(ctx context.Context, input *Input, output *Output) error {
	input.Init()
	if input.Workdir == "" {
		return fmt.Errorf("workdir is required")
	}
	// Get or create a session for this host
	session, err := s.getSession(ctx, input.Host, input.Env)
	if err != nil {
		return fmt.Errorf("failed to get session: %w", err)
	}
	defer session.close()

	// Set working directory if specified
	if input.Workdir != "" {
		_, _, err := session.service.Run(ctx, fmt.Sprintf("cd %s", input.Workdir))
		if err != nil {
			return fmt.Errorf("failed to change directory: %w", err)
		}
	}

	// Default abort on error to true if not specified
	abortOnError := true
	if input.AbortOnError != nil {
		abortOnError = *input.AbortOnError
	}

	// Execute all commands
	commands := make([]*Command, 0, len(input.Commands))
	var combinedStdout, combinedStderr strings.Builder
	var lastExitCode int

	timeoutDuration := time.Duration(input.TimeoutMs) * time.Millisecond
	if timeoutDuration == 0 {
		timeoutDuration = 3 * time.Minute
	}
	var errorCodeCmd string
	var lastErrorCode int
	for _, cmd := range input.Commands {
		command := &Command{
			Input: cmd,
		}
		stdout, stderr, exitCode := s.executeCommand(ctx, session, cmd, timeoutDuration)
		command.Output = stdout
		command.Stderr = stderr
		command.Status = exitCode
		commands = append(commands, command)

		if exitCode != 0 {
			lastErrorCode = exitCode
			errorCodeCmd = cmd
		}

		if stdout != "" {
			combinedStdout.WriteString(stdout)
			combinedStdout.WriteString("\n")
		}

		if stderr != "" {
			combinedStderr.WriteString(stderr)
			combinedStderr.WriteString("\n")
		}

		lastExitCode = exitCode

		// Stop execution if command failed and abort on error is true
		if abortOnError && exitCode != 0 {
			break
		}
	}

	// Set the output values
	output.Commands = commands
	output.Stdout = strings.TrimSpace(combinedStdout.String())
	output.Stderr = strings.TrimSpace(combinedStderr.String())
	output.Status = lastExitCode
	if lastErrorCode != 0 && output.Stderr == "" {
		output.Stderr = fmt.Sprintf("command %s exited with non-zero exit code", errorCodeCmd)
	}
	return nil
}

// executeCommand runs a single command and returns its output
func (s *Service) executeCommand(ctx context.Context, session *sessionInfo, command string, duration time.Duration) (string, string, int) {

	started := time.Now()
	stdout, status, err := session.service.Run(ctx, command, runner.WithTimeout(int(duration.Milliseconds())))
	elapsed := time.Now().Sub(started)
	if elapsed > duration && err == nil {
		err = fmt.Errorf("command %v timed out after: %s", command, elapsed.String())
		status = timeoutCode
		return stdout, err.Error(), status
	}

	if status == 0 {
		return stdout, "", status
	}
	if stdout == "" && err != nil {
		stdout = err.Error()
	}
	return "", stdout, status
}

// getSession retrieves an existing session or creates a new one
func (s *Service) getSession(ctx context.Context, host *system.Host, env map[string]string) (*sessionInfo, error) {
	sessionID := host.URL
	// Create new session
	var service *gosh.Service
	var err error

	// Set up environment options
	envOptions := []runner.Option{}
	if len(env) > 0 {
		envOptions = append(envOptions, runner.WithEnvironment(env))
	}
	// Local execution
	if url.Host(host.URL) == "localhost" {
		service, err = gosh.New(ctx, local.New(envOptions...))
	} else {

		// This assumes the secrets resource can provide SSH credentials
		// You'd need to implement the actual credential fetching logic
		config, err := s.getSSHConfig(ctx, host)
		if err != nil {
			return nil, fmt.Errorf("failed to get SSH config: %w", err)
		}

		sshHost := url.Host(host.URL)
		if !strings.Contains(sshHost, ":") {
			sshHost += ":22"
		}

		service, err = gosh.New(ctx, rssh.New(sshHost, config, envOptions...))
	}
	if err != nil {
		return nil, err
	}
	session := &sessionInfo{
		service: service,
		id:      sessionID,
		close: func() {
			_ = service.Close()
		},
	}
	//s.sessions[sessionID] = session
	return session, nil
}

// getSSHConfig creates an SSH config from the host's secrets
func (s *Service) getSSHConfig(ctx context.Context, host *system.Host) (*ssh.ClientConfig, error) {
	credentials := host.Credentials
	if credentials == "" {
		credentials = "localhost"
	}
	secrets := secret.New()
	generic, err := secrets.GetCredentials(ctx, credentials)
	if err != nil {
		return nil, err
	}
	return generic.SSH.Config(ctx)
}

// Close releases all sessions held by this service
func (s *Service) Close(ctx context.Context) error {
	return nil
}
