package secret

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/scy"
	sjwt "github.com/viant/scy/auth/jwt"
	"github.com/viant/scy/auth/jwt/verifier"
)

// VerifyJWTInput defines parameters for JWT verification
type VerifyJWTInput struct {
	Token      string `json:"token,omitempty" description:"JWT token to verify"`
	TokenURL   string `json:"tokenURL,omitempty" description:"URL to read JWT token from"`
	RSAKeyURL  string `json:"rsaKeyURL,omitempty" description:"URL of RSA public key to verify with"`
	HMACKeyURL string `json:"hmacKeyURL,omitempty" description:"URL of HMAC key to verify with (base64 encoded)"`
	KeySecret  string `json:"keySecret,omitempty" description:"Secret to decrypt key (if encrypted)"`
}

// VerifyJWTOutput contains verification results
type VerifyJWTOutput struct {
	Valid  bool         `json:"valid" description:"Whether the JWT token is valid"`
	Claims *sjwt.Claims `json:"claims,omitempty" description:"Claims from the verified JWT"`
}

// VerifyJWT verifies a JWT token and returns its claims
func (s *Service) VerifyJWT(ctx context.Context, input *VerifyJWTInput, output *VerifyJWTOutput) error {
	// Check that we have a verification key
	if input.RSAKeyURL == "" && input.HMACKeyURL == "" {
		return fmt.Errorf("either rsaKeyURL or hmacKeyURL must be provided")
	}

	// Create verifier configuration
	config := &verifier.Config{}
	if input.RSAKeyURL != "" {
		config.RSA = &scy.Resource{
			URL: input.RSAKeyURL,
			Key: input.KeySecret,
		}
	}
	if input.HMACKeyURL != "" {
		config.HMAC = &scy.Resource{
			URL: input.HMACKeyURL,
			Key: input.KeySecret,
		}
	}

	// Initialize JWT verifier
	jwtVerifier := verifier.New(config)
	if err := jwtVerifier.Init(ctx); err != nil {
		return fmt.Errorf("failed to initialize JWT verifier: %w", err)
	}

	// Get token
	var tokenString string
	if input.Token != "" {
		tokenString = input.Token
	} else if input.TokenURL != "" {
		fs := afs.New()
		tokenData, err := fs.DownloadWithURL(ctx, input.TokenURL)
		if err != nil {
			return fmt.Errorf("failed to download token from %s: %w", input.TokenURL, err)
		}
		tokenString = string(tokenData)
	} else {
		return fmt.Errorf("no token provided: specify token or tokenURL")
	}

	// Verify the JWT token
	claims, err := jwtVerifier.VerifyClaims(ctx, tokenString)
	if err != nil {
		output.Valid = false
		return nil // Not returning error as invalid token is a valid result
	}

	output.Valid = true
	output.Claims = claims
	return nil
}
