package storage

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"path"
)

// ListInput defines parameters for listing assets
type ListInput struct {
	URL       string `json:"url" required:"true" description:"URL to list files from"`
	Recursive bool   `json:"recursive,omitempty" description:"List files recursively"`
	PageSize  int    `json:"pageSize,omitempty" description:"Maximum number of results to return"`
}

// ListOutput contains results from a list operation
type ListOutput struct {
	Assets []*Asset `json:"assets,omitempty" description:"List of assets found"`
}

// List lists files and directories at the specified URL
func (s *Service) List(ctx context.Context, input *ListInput, output *ListOutput) error {
	fs := afs.New()

	if input.URL == "" {
		return fmt.Errorf("URL is required")
	}

	listOptions := make([]storage.Option, 0)
	// Add options for recursive listing
	if input.Recursive {
		listOptions = append(listOptions, option.NewRecursive(true))
	}

	// Add pagination if specified
	if input.PageSize > 0 {
		listOptions = append(listOptions, option.NewPage(0, input.PageSize))
	}

	objects, err := fs.List(ctx, input.URL, listOptions...)
	if err != nil {
		return fmt.Errorf("failed to list objects at %s: %w", input.URL, err)
	}

	assets := make([]*Asset, 0, len(objects))
	for _, obj := range objects {
		asset := &Asset{
			URL:         obj.URL(),
			Name:        path.Base(obj.URL()),
			IsDir:       obj.IsDir(),
			Size:        obj.Size(),
			ModTime:     obj.ModTime(),
			ContentType: GetContentType(url.Path(obj.URL())),
		}
		asset.Mode = obj.Mode().String()
		assets = append(assets, asset)
	}

	output.Assets = assets
	return nil
}
