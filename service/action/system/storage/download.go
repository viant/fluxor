package storage

import (
	"context"
	"fmt"
	"github.com/viant/afs/url"
	"path/filepath"
)

// DownloadInput defines parameters for downloading assets
type DownloadInput struct {
	Assets      []string `json:"assets" required:"true" description:"URLs of assets to download"`
	IncludeData bool     `json:"includeData,omitempty" description:"Include file data in response"`
	Dest        string   `json:"dest,omitempty" description:"Destination path"`
}

// DownloadOutput contains results from a download operation
type DownloadOutput struct {
	Assets []*Asset `json:"assets,omitempty" description:"Downloaded assets"`
}

// Download downloads assets from specified URLs
func (s *Service) Download(ctx context.Context, input *DownloadInput, output *DownloadOutput) error {
	if len(input.Assets) == 0 {
		return fmt.Errorf("at least one asset Location is required")
	}

	downloadedAssets := make([]*Asset, 0, len(input.Assets))

	for _, assetURL := range input.Assets {
		if assetURL == "" {
			continue
		}

		// Check if the file exists
		exists, err := s.fs.Exists(ctx, assetURL)
		if err != nil {
			return fmt.Errorf("failed to check if %s exists: %w", assetURL, err)
		}
		if !exists {
			return fmt.Errorf("asset does not exist: %s", assetURL)
		}

		// Get file source
		source, err := s.fs.Object(ctx, assetURL)
		if err != nil {
			return fmt.Errorf("failed to get source for %s: %w", assetURL, err)
		}

		// Skip directories if this is a single file request
		if source.IsDir() {
			return fmt.Errorf("cannot download directory, use list operation first: %s", assetURL)
		}

		asset := &Asset{
			Location:    assetURL,
			Name:        filepath.Base(assetURL),
			IsDir:       source.IsDir(),
			Size:        source.Size(),
			ModTime:     source.ModTime(),
			ContentType: GetContentType(url.Path(assetURL)),
		}

		asset.Mode = source.Mode().String()

		// Include file data if requested
		if input.IncludeData {
			data, err := s.fs.DownloadWithURL(ctx, assetURL)
			if err != nil {
				return fmt.Errorf("failed to download data from %s: %w", assetURL, err)
			}
			asset.Data = data
		}

		// Download to fs if destination is specified
		if input.Dest != "" {
			destPath := input.Dest

			// If destination is a directory, append filename
			object, _ := s.fs.Object(ctx, destPath)
			if object.IsDir() {
				destPath = url.Join(destPath, filepath.Base(assetURL))
			}
			// Download the file
			err := s.fs.Copy(ctx, assetURL, destPath)
			if err != nil {
				return fmt.Errorf("failed to copy %s to %s: %w", assetURL, destPath, err)
			}
		}

		downloadedAssets = append(downloadedAssets, asset)
	}

	output.Assets = downloadedAssets
	return nil
}
