package executor

// Session represents a terminal session
type Session struct {
	ID              string            // Unique session identifier
	CurrentDirectory string           // Current working directory
	EnvVariables    map[string]string // Environment variables set in this session
}

// NewSession creates a new session
func NewSession(id string) *Session {
	return &Session{
		ID:           id,
		EnvVariables: make(map[string]string),
	}
}
