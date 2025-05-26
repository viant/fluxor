package dao

import "errors"

// Common, reusable DAO errors.  Using sentinel variables allows callers to
// reliably detect error conditions via errors.Is/As instead of brittle string
// comparisons.

var (
	// ErrNotFound is returned when the requested entity does not exist in the
	// underlying storage.
	ErrNotFound = errors.New("dao: not found")

	// ErrInvalidID indicates that the supplied ID/key is empty or otherwise
	// invalid.
	ErrInvalidID = errors.New("dao: invalid id")

	// ErrNilEntity is returned when the caller attempts to persist a nil
	// pointer.
	ErrNilEntity = errors.New("dao: nil entity")
)
