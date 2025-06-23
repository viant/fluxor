// onpatch: transactional file patching service with rollback support.
//
// This package exposes:
//   - Session‑scoped file operations (Add, Delete, Move, Update)
//   - Diff generation (F‑04)
//   - Unified‑patch application (F‑03)
//
// Key change in this revision ➜ **each mutating call now stores its own unique
// backup snapshot**, preventing the original‑overwrite bug when the same file
// is patched multiple times within a single session.
//
// External deps (add to go.mod):
//
//	github.com/pmezard/go-difflib/difflib
//	github.com/sourcegraph/go-diff/diff
//
// Example:
//
//	s, _ := onpatch.NewSession()
//	_ = s.Update("foo.txt", []byte("v1\n"))
//	_ = s.Update("foo.txt", []byte("v2\n")) // second update gets its own backup
//	_ = s.Rollback() // restores original pre‑session content
package patch

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pmezard/go-difflib/difflib"
	sgdiff "github.com/sourcegraph/go-diff/diff"
)

// ──────────────────────────────────────────────────────────────────────────────
// Diff generation (F‑04)
// ──────────────────────────────────────────────────────────────────────────────

type DiffStats struct {
	FilesChanged int
	Insertions   int
	Deletions    int
	Hunks        int
}

type DiffResult struct {
	Patch string
	Stats DiffStats
}

var ErrNoChange = errors.New("no change between old and new")

func GenerateDiff(old, new []byte, path string, contextLines int) (DiffResult, error) {
	if bytes.Equal(old, new) {
		return DiffResult{}, ErrNoChange
	}
	if path == "" {
		path = "file"
	}
	if contextLines <= 0 {
		contextLines = 3
	}

	ud := difflib.UnifiedDiff{
		A:        difflib.SplitLines(string(old)),
		B:        difflib.SplitLines(string(new)),
		FromFile: "a/" + path,
		ToFile:   "b/" + path,
		Context:  contextLines,
	}

	patch, err := difflib.GetUnifiedDiffString(ud)
	if err != nil {
		return DiffResult{}, fmt.Errorf("diff generation: %w", err)
	}

	stats := DiffStats{FilesChanged: 1}
	for _, l := range strings.Split(patch, "\n") {
		switch {
		case strings.HasPrefix(l, "@@"):
			stats.Hunks++
		case strings.HasPrefix(l, "+") && !strings.HasPrefix(l, "+++"):
			stats.Insertions++
		case strings.HasPrefix(l, "-") && !strings.HasPrefix(l, "---"):
			stats.Deletions++
		}
	}

	return DiffResult{Patch: patch, Stats: stats}, nil
}

// ──────────────────────────────────────────────────────────────────────────────
// Session engine
// ──────────────────────────────────────────────────────────────────────────────

type Action string

const (
	Delete Action = "delete"
	Move   Action = "move"
	Update Action = "update"
	Add    Action = "add"
)

type rollbackEntry struct {
	action   Action
	path     string // primary path affected
	auxPath  string // destination for move, otherwise ""
	tempCopy string // unique snapshot path
}

type Session struct {
	ID        string
	tempDir   string
	rollbacks []rollbackEntry
	committed bool
	mu        sync.Mutex // guards committed flag and rollbacks slice
}

func NewSession() (*Session, error) {
	tmp, err := os.MkdirTemp("", "onpatch‑*")
	if err != nil {
		return nil, err
	}
	return &Session{ID: filepath.Base(tmp), tempDir: tmp}, nil
}

// backup now stores **one snapshot per invocation** using a timestamp‑suffix to
// avoid overwriting when the same file is modified multiple times.
func (s *Session) backup(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	rel := strings.TrimPrefix(path, string(os.PathSeparator))
	unique := fmt.Sprintf("%s.%d.bak", rel, time.Now().UnixNano())
	dst := filepath.Join(s.tempDir, unique)
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return "", err
	}
	if err := os.WriteFile(dst, data, 0o644); err != nil {
		return "", err
	}
	return dst, nil
}

func (s *Session) assertActive() error {
	if s.committed {
		return errors.New("session already committed")
	}
	return nil
}

func (s *Session) Delete(path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.assertActive(); err != nil {
		return err
	}
	if _, err := os.Stat(path); err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	backup, err := s.backup(path)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil {
		return err
	}
	s.rollbacks = append(s.rollbacks, rollbackEntry{action: Delete, path: path, tempCopy: backup})
	return nil
}

func (s *Session) Move(src, dst string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.assertActive(); err != nil {
		return err
	}
	if _, err := os.Stat(src); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	if err := os.Rename(src, dst); err != nil {
		return err
	}
	s.rollbacks = append(s.rollbacks, rollbackEntry{action: Move, path: src, auxPath: dst})
	return nil
}

func (s *Session) Update(path string, newData []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.assertActive(); err != nil {
		return err
	}
	if _, err := os.Stat(path); err != nil {
		return err
	}
	backup, err := s.backup(path)
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, newData, 0o644); err != nil {
		return err
	}
	s.rollbacks = append(s.rollbacks, rollbackEntry{action: Update, path: path, tempCopy: backup})
	return nil
}

func (s *Session) Add(path string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.assertActive(); err != nil {
		return err
	}
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("add: file %s already exists", path)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return err
	}
	s.rollbacks = append(s.rollbacks, rollbackEntry{action: Add, path: path})
	return nil
}

func (s *Session) Rollback() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := len(s.rollbacks) - 1; i >= 0; i-- {
		r := s.rollbacks[i]
		switch r.action {
		case Delete, Update:
			data, err := os.ReadFile(r.tempCopy)
			if err != nil {
				return err
			}
			if err := os.MkdirAll(filepath.Dir(r.path), 0o755); err != nil {
				return err
			}
			if err := os.WriteFile(r.path, data, 0o644); err != nil {
				return err
			}
		case Move:
			if err := os.Rename(r.auxPath, r.path); err != nil {
				return err
			}
		case Add:
			if err := os.Remove(r.path); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("rollback add: %w", err)
			}
		}
	}
	if err := os.RemoveAll(s.tempDir); err != nil {
		return fmt.Errorf("rollback cleanup: %w", err)
	}
	s.rollbacks = nil
	return nil
}

func (s *Session) Commit() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.committed {
		return nil
	}

	s.committed = true
	s.rollbacks = nil
	if err := os.RemoveAll(s.tempDir); err != nil {
		return fmt.Errorf("commit cleanup: %w", err)
	}
	return nil
}

// ──────────────────────────────────────────────────────────────────────────────
// Patch application (F‑03)
// ──────────────────────────────────────────────────────────────────────────────

func (s *Session) ApplyPatch(patchText string) error {
	mfd, err := sgdiff.ParseMultiFileDiff([]byte(patchText))
	if err != nil {
		return fmt.Errorf("parse patch: %w", err)
	}
	for _, fd := range mfd {
		orig := strings.TrimPrefix(fd.OrigName, "a/")
		newer := strings.TrimPrefix(fd.NewName, "b/")

		switch {
		case fd.NewName != "/dev/null" && fd.OrigName == "/dev/null":
			var buf bytes.Buffer
			if err := applyHunks(nil, fd.Hunks, &buf); err != nil {
				return err
			}
			if err := s.Add(newer, buf.Bytes()); err != nil {
				return err
			}
		case fd.NewName == "/dev/null" && fd.OrigName != "/dev/null":
			if err := s.Delete(orig); err != nil {
				return err
			}
		case orig != newer && len(fd.Hunks) == 0:
			if err := s.Move(orig, newer); err != nil {
				return err
			}
		default:
			oldData, err := os.ReadFile(orig)
			if err != nil {
				return err
			}
			var buf bytes.Buffer
			if err := applyHunks(oldData, fd.Hunks, &buf); err != nil {
				return err
			}
			target := orig
			if orig != newer {
				if err := s.Move(orig, newer); err != nil {
					return err
				}
				target = newer
			}
			if err := s.Update(target, buf.Bytes()); err != nil {
				return err
			}
		}
	}
	return nil
}

// applyHunks applies diff hunks to oldData and writes the patched file to w.
// It walks the original lines sequentially, verifies every context and delete
// line for consistency, and emits additions.  Any mismatch aborts with error.
func applyHunks(oldData []byte, hunks []*sgdiff.Hunk, w io.Writer) error {
	// Preserve original newline layout.
	oldLines := strings.SplitAfter(string(oldData), "\n")
	origIdx := 0 // 0-based index into oldLines

	linesEqual := func(a, b string) bool {
		if a == b {
			return true
		}
		// Handle newline-at-EOF equivalence: SplitAfter leaves an empty string as
		// the last slice element whereas diff encodes it as "\n" context line.
		if (a == "" && b == "\n") || (a == "\n" && b == "") {
			return true
		}
		return false
	}

	for _, h := range hunks {
		// 1) Copy untouched lines that appear before this hunk.
		//    OrigStartLine is 1-based; we want everything < that line.
		targetIdx := int(h.OrigStartLine) - 1
		for origIdx < targetIdx && origIdx < len(oldLines) {
			if _, err := io.WriteString(w, oldLines[origIdx]); err != nil {
				return err
			}
			origIdx++
		}

		// 2) Process the hunk body.
		for _, hl := range strings.SplitAfter(string(h.Body), "\n") {
			if hl == "" { // final split can be empty
				continue
			}
			tag := hl[0]
			line := hl[1:] // includes trailing newline (if present)

			switch tag {
			case ' ': // context — must match original, then copy through
				if origIdx >= len(oldLines) || !linesEqual(oldLines[origIdx], line) {
					return fmt.Errorf("patch failed: context mismatch at original line %d", origIdx+1)
				}
				// The special case where "line" is just "\n" and the counterpart in
				// oldLines is an empty string means we are at the implicit newline that
				// terminates the file. It has already been emitted as part of the
				// previous line, so we skip writing to avoid producing an extra blank
				// line (issue #thread-safe-newline).
				if !(oldLines[origIdx] == "" && line == "\n") {
					if _, err := io.WriteString(w, line); err != nil {
						return err
					}
				}
				origIdx++

			case '-': // deletion — must match original, *do not* copy
				if origIdx >= len(oldLines) || !linesEqual(oldLines[origIdx], line) {
					return fmt.Errorf("patch failed: delete mismatch at original line %d", origIdx+1)
				}
				origIdx++

			case '+': // addition — write to output, do not advance original
				if _, err := io.WriteString(w, line); err != nil {
					return err
				}

			case '\\': // “\ No newline at end of file” — ignore
				continue

			default:
				return fmt.Errorf("patch failed: unexpected hunk tag %q", tag)
			}
		}
	}

	// 3) Copy any remaining untouched lines after the last hunk.
	for origIdx < len(oldLines) {
		if _, err := io.WriteString(w, oldLines[origIdx]); err != nil {
			return err
		}
		origIdx++
	}
	return nil
}
