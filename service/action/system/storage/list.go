package storage

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
)

// ListInput defines parameters for listing assets
type ListInput struct {
	Location  string `json:"location" required:"true" description:"Location to list files from"`
	Recursive bool   `json:"recursive,omitempty" description:"List files recursively"`
	PageSize  int    `json:"pageSize,omitempty" description:"Maximum number of results to return"`
}

// ListOutput contains results from a list operation
type ListOutput struct {
	Assets []*Asset `json:"assets,omitempty" description:"List of assets found"`
}

// List lists files and directories at the specified Location
func (s *Service) List(ctx context.Context, input *ListInput, output *ListOutput) error {
	fs := afs.New()

	if input.Location == "" {
		return fmt.Errorf("Location is required")
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

	objects, err := fs.List(ctx, input.Location, listOptions...)
	if err != nil {
		return fmt.Errorf("failed to list objects at %s: %w", input.Location, err)
	}

	assets := make([]*Asset, 0, len(objects))
	for _, obj := range objects {
		location := obj.URL()
		if url.Scheme(location, file.Scheme) == file.Scheme {
			location = url.Path(location)
		}
		asset := &Asset{
			Location:    location,
			Name:        obj.Name(),
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
