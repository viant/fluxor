package secret

import (
	"context"
	"fmt"
	"github.com/viant/scy"
	"github.com/viant/scy/cred"
	"github.com/viant/toolbox"
)

// RevealInput defines parameters for revealing secrets
type RevealInput struct {
	SourceURL string `json:"sourceURL" required:"true" description:"URL to read the encrypted secret from"`
	Target    string `json:"target,omitempty" description:"Target credential type ('raw', 'basic', 'key', 'generic', etc.)"`
	Key       string `json:"key,omitempty" description:"Encryption key, e.g., 'blowfish://default'"`
}

// RevealOutput contains the revealed secret
type RevealOutput struct {
	PlainText string                 `json:"plainText,omitempty" description:"Decrypted content as string (for raw type)"`
	Data      map[string]interface{} `json:"data,omitempty" description:"Decrypted content as structured data (for typed secrets)"`
	Success   bool                   `json:"success" description:"Whether the operation succeeded"`
}

// Reveal decrypts a secret
func (s *Service) Reveal(ctx context.Context, input *RevealInput, output *RevealOutput) error {
	var target interface{} = nil

	// Determine target type
	if input.Target != "" && input.Target != "raw" {
		targetType, err := cred.TargetType(input.Target)
		if err != nil {
			return fmt.Errorf("invalid target type '%s': %w", input.Target, err)
		}
		if targetType != nil {
			target = targetType
		}
	}

	// Create resource and load secret
	resource := scy.NewResource(target, input.SourceURL, input.Key)
	secret, err := s.scyService.Load(ctx, resource)
	if err != nil {
		return fmt.Errorf("failed to load secret from %s: %w", input.SourceURL, err)
	}

	if !secret.IsPlain && secret.Target != nil {
		// For structured data, convert to map
		aMap := map[string]interface{}{}
		if err := toolbox.DefaultConverter.AssignConverted(&aMap, secret.Target); err != nil {
			return fmt.Errorf("failed to convert secret data: %w", err)
		}

		// Remove empty fields
		aMap = toolbox.DeleteEmptyKeys(aMap)
		output.Data = aMap
	} else {
		// For raw content, return as string
		output.PlainText = secret.String()
	}

	output.Success = true
	return nil
}
