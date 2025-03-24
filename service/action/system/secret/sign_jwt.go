package secret

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/scy"
	"github.com/viant/scy/auth/jwt/signer"
	"time"
)

// SignJWTInput defines parameters for JWT signing
type SignJWTInput struct {
	Claims         map[string]interface{} `json:"claims,omitempty" description:"Claims to include in the JWT"`
	ClaimsURL      string                 `json:"claimsURL,omitempty" description:"URL to read claims JSON from"`
	RSAKeyURL      string                 `json:"rsaKeyURL,omitempty" description:"URL of RSA key to sign with"`
	HMACKeyURL     string                 `json:"hmacKeyURL,omitempty" description:"URL of HMAC key to sign with (base64 encoded)"`
	KeySecret      string                 `json:"keySecret,omitempty" description:"Secret to decrypt key (if encrypted)"`
	ExpiryDuration int                    `json:"expiryDuration,omitempty" description:"Token expiry duration in seconds (default: 3600)"`
}

// SignJWTOutput contains the signed JWT
type SignJWTOutput struct {
	Token   string `json:"token" description:"Signed JWT token"`
	Success bool   `json:"success" description:"Whether the operation succeeded"`
}

// SignJWT creates a signed JWT token
func (s *Service) SignJWT(ctx context.Context, input *SignJWTInput, output *SignJWTOutput) error {
	// Check that we have a signing key
	if input.RSAKeyURL == "" && input.HMACKeyURL == "" {
		return fmt.Errorf("either rsaKeyURL or hmacKeyURL must be provided")
	}

	// Create signer configuration
	config := &signer.Config{}
	if input.RSAKeyURL != "" {
		config.RSA = &scy.Resource{
			URL: input.RSAKeyURL,
			Key: input.KeySecret,
		}
	} else {
		config.HMAC = &scy.Resource{
			URL: input.HMACKeyURL,
			Key: input.KeySecret,
		}
	}

	// Initialize JWT signer
	jwtSigner := signer.New(config)
	if err := jwtSigner.Init(ctx); err != nil {
		return fmt.Errorf("failed to initialize JWT signer: %w", err)
	}

	// Get claims data
	var claims map[string]interface{}
	if len(input.Claims) > 0 {
		claims = input.Claims
	} else if input.ClaimsURL != "" {
		fs := afs.New()
		data, err := fs.DownloadWithURL(ctx, input.ClaimsURL)
		if err != nil {
			return fmt.Errorf("failed to download claims from %s: %w", input.ClaimsURL, err)
		}

		if err = json.Unmarshal(data, &claims); err != nil {
			return fmt.Errorf("invalid JSON claims: %w", err)
		}
	} else {
		return fmt.Errorf("no claims provided: specify claims or claimsURL")
	}

	// Set expiry duration
	expiryDuration := time.Duration(input.ExpiryDuration) * time.Second
	if expiryDuration == 0 {
		expiryDuration = time.Hour // Default to 1 hour
	}

	// Create the JWT token
	token, err := jwtSigner.Create(expiryDuration, claims)
	if err != nil {
		return fmt.Errorf("failed to create JWT token: %w", err)
	}

	output.Token = token
	output.Success = true
	return nil
}
