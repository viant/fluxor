package correlation

import (
	"strings"
	"sync"
	"time"
)

// Group represents a rendez-vous for a set of asynchronous executions emitted
// by a parent task.  The group tracks how many children were expected and how
// many have already reported completion.
type Group struct {
	ID              string
	ParentProcessID string
	ParentExecID    string

	Expected int

	mu        sync.Mutex
	completed int
	failed    int

	Outputs []interface{}

	DoneAt    *time.Time
	TimeoutAt *time.Time // nil means no timeout

	Mode  string
	Merge string
}

// Failed returns true when at least one child reported failure.
func (g *Group) Failed() bool {
	g.mu.Lock()
	f := g.failed > 0
	g.mu.Unlock()
	return f
}

// AggregateOutputs returns slice of collected child outputs.
func (g *Group) AggregateOutputs() []interface{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]interface{}(nil), g.Outputs...)
}

// MarkDone registers the completion of a child execution and returns true
// when the rendez-vous condition (all children finished) has been satisfied.
// pass failed=true if the child ended in error.
func (g *Group) MarkDone(failed bool, output interface{}) (groupComplete bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if failed {
		g.failed++
	}
	if output != nil {
		g.Outputs = append(g.Outputs, output)
	}
	g.completed++

	// Evaluate completion based on Mode
	switch strings.ToLower(g.Mode) {
	case "first":
		if g.DoneAt == nil {
			now := time.Now()
			g.DoneAt = &now
			return true
		}
	case "anyerror":
		if failed {
			if g.DoneAt == nil {
				now := time.Now()
				g.DoneAt = &now
			}
			return true
		}
		if g.completed >= g.Expected && g.Expected > 0 && g.DoneAt == nil {
			now := time.Now()
			g.DoneAt = &now
			return true
		}
	default: // "all" behaviour
		if g.completed >= g.Expected && g.Expected > 0 && g.DoneAt == nil {
			now := time.Now()
			g.DoneAt = &now
			return true
		}
	}
	return false
}

// Done returns whether the group has completed.
func (g *Group) Done() bool {
	g.mu.Lock()
	done := g.DoneAt != nil
	g.mu.Unlock()
	return done
}

// TimedOut returns true if TimeoutAt is set and now past it.
func (g *Group) TimedOut() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.TimeoutAt == nil {
		return false
	}
	return time.Now().After(*g.TimeoutAt) && g.DoneAt == nil
}
