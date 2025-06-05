package storage

import (
	"context"
	"github.com/viant/afs"
	"github.com/viant/fluxor/model/types"
	"reflect"
	"strings"
)

const Name = "system/storage"

// Service provides file system operations using viant/afs
type Service struct {
	fs afs.Service
}

// New creates a new storage service
func New() *Service {
	return &Service{fs: afs.New()}
}

// Name returns the service Name
func (s *Service) Name() string {
	return Name
}

// Methods returns the service methods
func (s *Service) Methods() types.Signatures {
	return []types.Signature{
		{
			Name:        "list",
			Description: "Lists files and directories at the specified URL.",
			Input:       reflect.TypeOf(&ListInput{}),
			Output:      reflect.TypeOf(&ListOutput{}),
		},
		{
			Name:        "download",
			Description: "Downloads assets from specified URLs and optionally writes data to a destination path or includes it in the response.",
			Input:       reflect.TypeOf(&DownloadInput{}),
			Output:      reflect.TypeOf(&DownloadOutput{}),
		},
		{
			Name:        "upload",
			Description: "Uploads provided assets to their specified URLs.",
			Input:       reflect.TypeOf(&UploadInput{}),
			Output:      reflect.TypeOf(&UploadOutput{}),
		},
	}
}

// Method returns the specified method
func (s *Service) Method(name string) (types.Executable, error) {
	switch strings.ToLower(name) {
	case "list":
		return s.list, nil
	case "download":
		return s.download, nil
	case "upload":
		return s.upload, nil
	default:
		return nil, types.NewMethodNotFoundError(name)
	}
}

// list handles file listing operations
func (s *Service) list(ctx context.Context, in, out interface{}) error {
	input, ok := in.(*ListInput)
	if !ok {
		return types.NewInvalidInputError(in)
	}
	output, ok := out.(*ListOutput)
	if !ok {
		return types.NewInvalidOutputError(out)
	}
	return s.List(ctx, input, output)
}

// download handles file download operations
func (s *Service) download(ctx context.Context, in, out interface{}) error {
	input, ok := in.(*DownloadInput)
	if !ok {
		return types.NewInvalidInputError(in)
	}
	output, ok := out.(*DownloadOutput)
	if !ok {
		return types.NewInvalidOutputError(out)
	}
	return s.Download(ctx, input, output)
}

// upload handles file upload operations
func (s *Service) upload(ctx context.Context, in, out interface{}) error {
	input, ok := in.(*UploadInput)
	if !ok {
		return types.NewInvalidInputError(in)
	}
	output, ok := out.(*UploadOutput)
	if !ok {
		return types.NewInvalidOutputError(out)
	}
	return s.Upload(ctx, input, output)
}
