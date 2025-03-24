package secret

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/scy"
	"github.com/viant/scy/cred"
	"reflect"
)

// SecureInput defines parameters for securing secrets
type SecureInput struct {
	SourceURL string                 `json:"sourceURL,omitempty" description:"URL to read the secret from (if content is not provided)"`
	Content   string                 `json:"content,omitempty" description:"Raw content to encrypt (if sourceURL is not provided)"`
	Data      map[string]interface{} `json:"data,omitempty" description:"JSON data to encrypt (if sourceURL and content are not provided)"`
	DestURL   string                 `json:"destURL" required:"true" description:"Destination URL where to store the encrypted secret"`
	Target    string                 `json:"target,omitempty" description:"Target credential type ('raw', 'basic', 'key', 'generic', etc.)"`
	Key       string                 `json:"key,omitempty" description:"Encryption key, e.g., 'blowfish://default'"`
}

// SecureOutput contains results from encrypting a secret
type SecureOutput struct {
	Success bool   `json:"success" description:"Whether the operation succeeded"`
	Message string `json:"message,omitempty" description:"Optional result message"`
}

// Secure encrypts and stores a secret
func (s *Service) Secure(ctx context.Context, input *SecureInput, output *SecureOutput) error {
	var data []byte
	var err error

	// Determine source of data to encrypt
	if input.Content != "" {
		data = []byte(input.Content)
	} else if len(input.Data) > 0 {
		data, err = json.Marshal(input.Data)
		if err != nil {
			return fmt.Errorf("failed to marshal data: %w", err)
		}
	} else if input.SourceURL != "" {
		fs := afs.New()
		data, err = fs.DownloadWithURL(ctx, input.SourceURL)
		if err != nil {
			return fmt.Errorf("failed to download from %s: %w", input.SourceURL, err)
		}
	} else {
		return fmt.Errorf("no content provided: specify sourceURL, content, or data")
	}

	// Determine target type
	var targetType reflect.Type
	if input.Target != "" && input.Target != "raw" {
		targetType, err = cred.TargetType(input.Target)
		if err != nil {
			return fmt.Errorf("invalid target type '%s': %w", input.Target, err)
		}
	}

	var secret *scy.Secret
	if targetType != nil {
		// Create instance of target type and unmarshal data into it
		instance := reflect.New(targetType).Interface()
		if err := json.Unmarshal(data, instance); err != nil {
			return fmt.Errorf("failed to unmarshal data to target type %s: %w", input.Target, err)
		}

		resource := scy.NewResource(targetType, input.DestURL, input.Key)
		secret = scy.NewSecret(instance, resource)
	} else {
		// Use raw string content
		resource := scy.NewResource(nil, input.DestURL, input.Key)
		secret = scy.NewSecret(string(data), resource)
	}

	// Store the encrypted secret
	if err := s.scyService.Store(ctx, secret); err != nil {
		return fmt.Errorf("failed to store encrypted secret: %w", err)
	}

	output.Success = true
	output.Message = fmt.Sprintf("Secret successfully encrypted and stored at %s", input.DestURL)
	return nil
}
