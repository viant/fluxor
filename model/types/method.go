package types

import (
	"context"
	"reflect"
)

type Signatures []Signature

func (s Signatures) Lookup(name string) *Signature {
	for i := range s {
		sig := &s[i]
		if sig.Name == name {
			return sig
		}
	}
	return nil
}

// Signature	method signature
type Signature struct {
	Name   string
	Input  reflect.Type
	Output reflect.Type
}

// Executable is a function that can be executed
type Executable func(context context.Context, input, output interface{}) error
