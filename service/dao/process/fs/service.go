package fs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/option"
	"github.com/viant/afs/url"
	"github.com/viant/fluxor/model/execution"
	"github.com/viant/fluxor/service/dao"
	"github.com/viant/fluxor/service/dao/criteria"
	"path"
	"strings"
	"sync"
)

// Service implements a filesystem-based process storage
type Service struct {
	basePath string
	fs       afs.Service
	mu       sync.RWMutex
}

// Ensure Service implements dao.Service
var _ dao.Service[string, execution.Process] = (*Service)(nil)

// Save persists a process to the filesystem
func (s *Service) Save(ctx context.Context, process *execution.Process) error {
	if process == nil {
		return fmt.Errorf("cannot save nil process")
	}
	if process.ID == "" {
		return fmt.Errorf("process ID cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := json.Marshal(process)
	if err != nil {
		return fmt.Errorf("failed to marshal process: %w", err)
	}

	filePath := s.processPath(process.ID)
	err = s.fs.Upload(ctx, filePath, file.DefaultFileOsMode, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to save process to file %s: %w", filePath, err)
	}

	return nil
}

// Load retrieves a process from the filesystem
func (s *Service) Load(ctx context.Context, id string) (*execution.Process, error) {
	if id == "" {
		return nil, fmt.Errorf("process ID cannot be empty")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	filePath := s.processPath(id)
	exists, err := s.fs.Exists(ctx, filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to check if process exists: %w", err)
	}

	if !exists {
		return nil, fmt.Errorf("process not found: %s", id)
	}

	data, err := s.fs.DownloadWithURL(ctx, filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read process file: %w", err)
	}

	var process execution.Process
	if err := json.Unmarshal(data, &process); err != nil {
		return nil, fmt.Errorf("failed to unmarshal process data: %w", err)
	}

	return &process, nil
}

// Delete removes a process from the filesystem
func (s *Service) Delete(ctx context.Context, id string) error {
	if id == "" {
		return fmt.Errorf("process ID cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	filePath := s.processPath(id)
	exists, err := s.fs.Exists(ctx, filePath)
	if err != nil {
		return fmt.Errorf("failed to check if process exists: %w", err)
	}

	if !exists {
		return fmt.Errorf("process not found: %s", id)
	}

	if err := s.fs.Delete(ctx, filePath); err != nil {
		return fmt.Errorf("failed to delete process file: %w", err)
	}

	return nil
}

// List returns all processes from the filesystem
func (s *Service) List(ctx context.Context, parameters ...*dao.Parameter) ([]*execution.Process, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	objects, err := s.fs.List(ctx, s.basePath, option.NewRecursive(true))
	if err != nil {
		return nil, fmt.Errorf("failed to list process files: %w", err)
	}

	var processes []*execution.Process
	for _, object := range objects {
		if object.IsDir() {
			continue
		}

		// Only process .json files
		if !strings.HasSuffix(object.Name(), ".json") {
			continue
		}

		data, err := s.fs.Download(ctx, object)
		if err != nil {
			// Log error but continue processing other files
			fmt.Printf("Error reading process file %s: %v\n", object.URL(), err)
			continue
		}

		var process execution.Process
		if err := json.Unmarshal(data, &process); err != nil {
			// Log error but continue processing other files
			fmt.Printf("Error unmarshaling process from %s: %v\n", object.URL(), err)
			continue
		}
		if !criteria.FilterByState(process.State, parameters) {
			continue
		}

		processes = append(processes, &process)
	}

	return processes, nil
}

// processPath returns the file path for a process
func (s *Service) processPath(id string) string {
	return path.Join(s.basePath, fmt.Sprintf("%s.json", id))
}

// New creates a new filesystem process storage service
func New(basePath string) (*Service, error) {
	if basePath == "" {
		return nil, fmt.Errorf("base path cannot be empty")
	}

	fs := afs.New()

	// Ensure the base directory exists
	ctx := context.Background()
	exists, _ := fs.Exists(ctx, basePath)
	if !exists {
		if err := fs.Create(ctx, basePath, file.DefaultDirOsMode, true); err != nil {
			return nil, fmt.Errorf("failed to create base directory: %w", err)
		}
	}

	// Normalize path
	basePath = url.Normalize(basePath, file.Scheme)

	return &Service{
		basePath: basePath,
		fs:       fs,
	}, nil
}
