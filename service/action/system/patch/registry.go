package patch

import (
	"context"
	_ "embed"
	"reflect"
	"strings"
	"sync"

	"github.com/viant/fluxor/model/types"
)

// Name of the system/patch action service.
const Name = "system/patch"

//go:embed apply_spec.md
var applySpec string

// Service exposes filesystem patching capabilities as a Fluxor action service.
// It is stateless – every method call operates with its own ephemeral Session.
type Service struct {
	mu      sync.Mutex
	session *Session
}

// New creates the patch service instance.
func New() *Service { return &Service{} }

// Name returns service identifier.
func (s *Service) Name() string { return Name }

// Methods returns service method catalogue.
func (s *Service) Methods() types.Signatures {
	return []types.Signature{
		{
			Name:        "apply",
			Description: "Applies custom patch.\n" + applySpec,
			Input:       reflect.TypeOf(&ApplyInput{}),
			Output:      reflect.TypeOf(&ApplyOutput{}),
		},
		{
			Name:        "diff",
			Description: "Generates a diff from two text blobs.",
			Input:       reflect.TypeOf(&DiffInput{}),
			Output:      reflect.TypeOf(&DiffOutput{}),
		},
		{
			Name:        "commit",
			Description: "Commits  discards the rollback information, clears session.",
			Input:       reflect.TypeOf(&EmptyInput{}),
			Output:      reflect.TypeOf(&EmptyOutput{}),
		},
		{
			Name:        "rollback",
			Description: "Rolls back all pending changes in the current patch session and clears the session.",
			Input:       reflect.TypeOf(&EmptyInput{}),
			Output:      reflect.TypeOf(&EmptyOutput{}),
		},
	}
}

// Method maps method names to executable handlers.
func (s *Service) Method(name string) (types.Executable, error) {
	switch strings.ToLower(name) {
	case "apply":
		return s.apply, nil
	case "diff":
		return s.diff, nil
	case "commit":
		return s.commit, nil
	case "rollback":
		return s.rollback, nil
	default:
		return nil, types.NewMethodNotFoundError(name)
	}
}

// -------------------------------------------------------------------------
// I/O contracts
// -------------------------------------------------------------------------

// ApplyInput is the payload for Service.apply
type ApplyInput struct {
	// The service applies the patch relative to the current working directory of the Fluxor runtime.
	Patch string `json:"patch" description:"Patch text to apply (either unified-diff format or simplified patch format)"`
	// Directory is the base directory for resolving relative paths in the patch.
	// If not provided, paths are resolved relative to the current working directory.
	Workdir string `json:"workdir" required:"true" description:"Workdir to relative paths in the patch"`
}

// ApplyOutput summarises the changes applied.
type ApplyOutput struct {
	Stats  DiffStats `json:"stats,omitempty"`
	Status string    `json:"status,omitempty"`
	Error  string    `json:"error,omitempty"`
}

// DiffInput is the payload for Service.diff
type DiffInput struct {
	OldContent   string `json:"old" description:"Original file content"`
	NewContent   string `json:"new" description:"Updated file content"`
	Path         string `json:"path,omitempty" description:"Display path for diff headers"`
	ContextLines int    `json:"contextLines,omitempty" description:"Number of context lines to include in diff (default 3)"`
}

// DiffOutput is identical to DiffResult, re-exported for JSON tags.
type DiffOutput DiffResult

// EmptyInput/Output used by commit/rollback methods.
type EmptyInput struct{}
type EmptyOutput struct{}

// -------------------------------------------------------------------------
// method executors
// -------------------------------------------------------------------------

func (s *Service) apply(ctx context.Context, in, out interface{}) error {
	input, ok := in.(*ApplyInput)
	if !ok {
		return types.NewInvalidInputError(in)
	}
	output, ok := out.(*ApplyOutput)
	if !ok {
		return types.NewInvalidOutputError(out)
	}
	output.Status = "ok"
	err := s.applyPatch(ctx, input, output)
	if err != nil {
		output.Error = err.Error()
		output.Status = "error"
	}
	return nil
}

func (s *Service) applyPatch(ctx context.Context, input *ApplyInput, output *ApplyOutput) error {
	s.mu.Lock()
	if s.session == nil {
		var err error
		s.session, err = NewSession()
		if err != nil {
			s.mu.Unlock()
			return err
		}
	}
	sess := s.session
	s.mu.Unlock()

	if err := sess.ApplyPatch(ctx, input.Patch, input.Workdir); err != nil {
		// rollback session and clear it
		_ = sess.Rollback(ctx)
		s.mu.Lock()
		s.session = nil
		s.mu.Unlock()
		return err
	}

	// Compute basic stats for user feedback.
	output.Stats = patchStats(input.Patch)
	// Session remains open for further apply calls until commit/rollback.
	return nil
}

// commit finalises the active session and clears it.
func (s *Service) commit(ctx context.Context, in, out interface{}) error {
	if _, ok := in.(*EmptyInput); !ok {
		return types.NewInvalidInputError(in)
	}
	if _, ok := out.(*EmptyOutput); !ok {
		return types.NewInvalidOutputError(out)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.session == nil {
		return nil // nothing to commit
	}
	err := s.session.Commit(ctx)
	s.session = nil
	return err
}

// rollback aborts the active session and clears it.
func (s *Service) rollback(ctx context.Context, in, out interface{}) error {
	if _, ok := in.(*EmptyInput); !ok {
		return types.NewInvalidInputError(in)
	}
	if _, ok := out.(*EmptyOutput); !ok {
		return types.NewInvalidOutputError(out)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.session == nil {
		return nil // nothing to rollback
	}
	err := s.session.Rollback(ctx)
	s.session = nil
	return err
}

func (s *Service) diff(ctx context.Context, in, out interface{}) error {
	input, ok := in.(*DiffInput)
	if !ok {
		return types.NewInvalidInputError(in)
	}
	output, ok := out.(*DiffOutput)
	if !ok {
		return types.NewInvalidOutputError(out)
	}

	// Context is not used in GenerateDiff, but we're keeping it for consistency
	res, stats, err := GenerateDiff([]byte(input.OldContent), []byte(input.NewContent), input.Path, input.ContextLines)
	if err != nil {
		return err
	}
	output.Patch = res
	output.Stats = stats
	return nil
}

// patchStats extracts basic statistics from a unified-diff string.
func patchStats(p string) DiffStats {
	stats := DiffStats{}
	for _, l := range strings.Split(p, "\n") {
		switch {
		case strings.HasPrefix(l, "+") && !strings.HasPrefix(l, "+++"):
			stats.Added++
		case strings.HasPrefix(l, "-") && !strings.HasPrefix(l, "---"):
			stats.Removed++
		}
	}
	return stats
}
