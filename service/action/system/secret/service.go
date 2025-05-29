package secret

import (
	"context"
	"github.com/viant/fluxor/model/types"
	"github.com/viant/scy"
	"reflect"
	"strings"
)

const Name = "system/secret"

// Service provides secret management operations using viant/scy
type Service struct {
	scyService *scy.Service
}

// New creates a new secret service
func New() *Service {
	return &Service{
		scyService: scy.New(),
	}
}

// Name returns the service Name
func (s *Service) Name() string {
	return Name
}

// Methods returns the service methods
func (s *Service) Methods() types.Signatures {
	return []types.Signature{
		{
			Name:   "secure",
			Input:  reflect.TypeOf(&SecureInput{}),
			Output: reflect.TypeOf(&SecureOutput{}),
		},
		{
			Name:   "reveal",
			Input:  reflect.TypeOf(&RevealInput{}),
			Output: reflect.TypeOf(&RevealOutput{}),
		},
		{
			Name:   "signJWT",
			Input:  reflect.TypeOf(&SignJWTInput{}),
			Output: reflect.TypeOf(&SignJWTOutput{}),
		},
		{
			Name:   "verifyJWT",
			Input:  reflect.TypeOf(&VerifyJWTInput{}),
			Output: reflect.TypeOf(&VerifyJWTOutput{}),
		},
	}
}

// Method returns the specified method
func (s *Service) Method(name string) (types.Executable, error) {
	switch strings.ToLower(name) {
	case "secure":
		return s.secure, nil
	case "reveal":
		return s.reveal, nil
	case "signjwt":
		return s.signJWT, nil
	case "verifyjwt":
		return s.verifyJWT, nil
	default:
		return nil, types.NewMethodNotFoundError(name)
	}
}

// secure handles secret encryption operations
func (s *Service) secure(ctx context.Context, in, out interface{}) error {
	input, ok := in.(*SecureInput)
	if !ok {
		return types.NewInvalidInputError(in)
	}
	output, ok := out.(*SecureOutput)
	if !ok {
		return types.NewInvalidOutputError(out)
	}
	return s.Secure(ctx, input, output)
}

// reveal handles secret decryption operations
func (s *Service) reveal(ctx context.Context, in, out interface{}) error {
	input, ok := in.(*RevealInput)
	if !ok {
		return types.NewInvalidInputError(in)
	}
	output, ok := out.(*RevealOutput)
	if !ok {
		return types.NewInvalidOutputError(out)
	}
	return s.Reveal(ctx, input, output)
}

// signJWT handles JWT signing operations
func (s *Service) signJWT(ctx context.Context, in, out interface{}) error {
	input, ok := in.(*SignJWTInput)
	if !ok {
		return types.NewInvalidInputError(in)
	}
	output, ok := out.(*SignJWTOutput)
	if !ok {
		return types.NewInvalidOutputError(out)
	}
	return s.SignJWT(ctx, input, output)
}

// verifyJWT handles JWT verification operations
func (s *Service) verifyJWT(ctx context.Context, in, out interface{}) error {
	input, ok := in.(*VerifyJWTInput)
	if !ok {
		return types.NewInvalidInputError(in)
	}
	output, ok := out.(*VerifyJWTOutput)
	if !ok {
		return types.NewInvalidOutputError(out)
	}
	return s.VerifyJWT(ctx, input, output)
}
