// Package policy provides a simple, optional per-action approval layer that can
// be attached to a workflow execution via context.  It is deliberately decoupled
// from the rest of Fluxor so that using it is entirely opt-in – engines that do
// not embed the Policy in their context keep the original "auto" behaviour.

package policy

import (
	"context"
	"strings"
)

// Execution modes recognised by the engine.
const (
	ModeAsk  = "ask"  // ask user before every action
	ModeAuto = "auto" // execute automatically (default)
	ModeDeny = "deny" // block execution
)

// AskFunc is invoked when Mode==ask.  Returning true approves the action, false
// rejects it.  Implementations MAY mutate the policy (for example, switching to
// ModeAuto after the first approval).
type AskFunc func(
	ctx context.Context,
	action string, // service.method
	args map[string]interface{}, // expanded input parameters – may be nil
	p *Policy,
) bool

// Policy represents the approval / debugging settings for the current
// workflow-run.
//
//   - Mode controls the high-level behaviour (ask / auto / deny).
//   - AllowList, BlockList allow coarse filtering regardless of Mode.
//   - Ask is only used when Mode==ask.
//
// A nil *Policy means "execute everything automatically" and is therefore the
// zero-cost default.
type Policy struct {
	Mode      string   // ask / auto / deny      (default = auto)
	AllowList []string // whitelist (empty => all)
	BlockList []string // blacklist
	Ask       AskFunc  // used only when Mode==ask
}

// ---------------------------------------------------------------------------
// Config <-> Policy converters (Config is a serialisable subset used when a
// Policy with AskFunc cannot be persisted).
// ---------------------------------------------------------------------------

// Config represents the declarative, serialisable part of a Policy.
type Config struct {
	Mode      string   `json:"mode,omitempty" yaml:"mode,omitempty"`
	AllowList []string `json:"allow,omitempty" yaml:"allow,omitempty"`
	BlockList []string `json:"block,omitempty" yaml:"block,omitempty"`
}

// ToConfig converts a runtime Policy into a persistable Config.
func ToConfig(p *Policy) *Config {
	if p == nil {
		return nil
	}
	return &Config{
		Mode:      p.Mode,
		AllowList: append([]string(nil), p.AllowList...),
		BlockList: append([]string(nil), p.BlockList...),
	}
}

// FromConfig converts a stored Config back to a runtime Policy (without
// AskFunc).
func FromConfig(c *Config) *Policy {
	if c == nil {
		return nil
	}
	return &Policy{
		Mode:      c.Mode,
		AllowList: append([]string(nil), c.AllowList...),
		BlockList: append([]string(nil), c.BlockList...),
	}
}

// IsAllowed evaluates AllowList / BlockList.  Both lists match by exact string
// comparison (case-insensitive) of the fully-qualified action name
// "service.method".
func (p *Policy) IsAllowed(action string) bool {
	if p == nil {
		return true
	}

	normalized := strings.ToLower(action)

	// BlockList has priority.
	for _, b := range p.BlockList {
		if normalized == strings.ToLower(b) {
			return false
		}
	}

	// AllowList – if empty everything is allowed, otherwise only the listed
	// entries.
	if len(p.AllowList) == 0 {
		return true
	}

	for _, a := range p.AllowList {
		if normalized == strings.ToLower(a) {
			return true
		}
	}

	return false
}

// ---------------------------------------------------------------------------
// Context helpers
// ---------------------------------------------------------------------------

type ctxKeyT struct{}

var ctxKey ctxKeyT

// WithPolicy embeds policy in ctx.
func WithPolicy(ctx context.Context, p *Policy) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, ctxKey, p)
}

// FromContext extracts (*Policy, ok).
func FromContext(ctx context.Context) *Policy {
	if ctx == nil {
		return nil
	}
	if v, ok := ctx.Value(ctxKey).(*Policy); ok {
		return v
	}
	return nil
}
