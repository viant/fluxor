package types

import "fmt"

func NewMethodNotFoundError(name string) error {
	return fmt.Errorf("method %v not found", name)
}

func NewInvalidInputError(in interface{}) error {
	return fmt.Errorf("invalid input %T", in)
}

func NewInvalidOutputError(in interface{}) error {
	return fmt.Errorf("invalid output %T", in)
}
