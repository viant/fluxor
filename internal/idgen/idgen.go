package idgen

import "github.com/google/uuid"

// New returns a new globally unique identifier as string. It is implemented
// as a thin wrapper so tests can stub it.

var NewFunc = func() string { return uuid.New().String() }

func New() string { return NewFunc() }
