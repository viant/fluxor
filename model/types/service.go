package types

// Service is a service interface
type Service interface {
	Name() string
	Methods() Signatures
	Method(name string) (Executable, error)
}

type Proxy func(base Service) Service
