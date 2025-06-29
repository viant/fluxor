package storage

import (
	"bytes"
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"path/filepath"
)

// UploadInput defines parameters for uploading assets
type UploadInput struct {
	Assets []*Asset `json:"assets" required:"true" description:"Assets to upload"`
}

// UploadOutput contains results from an upload operation
type UploadOutput struct {
	Assets []*Asset `json:"assets,omitempty" description:"Uploaded assets"`
}

// Upload uploads assets to their specified URLs
func (s *Service) Upload(ctx context.Context, input *UploadInput, output *UploadOutput) error {
	if len(input.Assets) == 0 {
		return fmt.Errorf("at least one asset is required for upload")
	}

	fs := afs.New()
	uploadedAssets := make([]*Asset, 0, len(input.Assets))

	for _, asset := range input.Assets {
		if asset.Location == "" {
			return fmt.Errorf("asset Location cannot be empty")
		}

		err := fs.Upload(ctx, asset.Location, file.DefaultFileOsMode, bytes.NewReader(asset.Data))
		if err != nil {
			return err
		}
		object, err := fs.Object(ctx, asset.Location)
		if err != nil {
			return fmt.Errorf("failed to get object for %s: %w", asset.Location, err)
		}
		uploadedAsset := &Asset{
			Location:    asset.Location,
			Name:        filepath.Base(asset.Location),
			Size:        object.Size(),
			ModTime:     object.ModTime(),
			Mode:        object.Mode().String(),
			ContentType: GetContentType(url.Path(asset.Location)),
		}
		uploadedAssets = append(uploadedAssets, uploadedAsset)
	}

	output.Assets = uploadedAssets
	return nil
}
