package fs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"github.com/viant/fluxor/service/messaging"
	"path"
	"strings"
	"sync"
	"time"
)

// MessageState represents the state of a message in the filesystem queue
type MessageState string

const (
	// MessageStatePending indicates a message is waiting to be processed
	MessageStatePending MessageState = "pending"

	// MessageStateProcessing indicates a message is being processed
	MessageStateProcessing MessageState = "processing"

	// MessageStateCompleted indicates a message was successfully processed
	MessageStateCompleted MessageState = "completed"

	// MessageStateFailed indicates a message failed processing
	MessageStateFailed MessageState = "failed"
)

// Message implements mbus.Message interface for filesystem queue
type Message[T any] struct {
	ID        string       `json:"id"`
	Data      T            `json:"data"`
	State     MessageState `json:"state"`
	Error     string       `json:"error,omitempty"`
	CreatedAt time.Time    `json:"createdAt"`
	UpdatedAt time.Time    `json:"updatedAt"`
	Retries   int          `json:"retries"`

	queue     *Queue[T]
	processed bool
	mu        sync.Mutex
}

// T returns the message payload
func (m *Message[T]) T() *T {
	return &m.Data
}

// Ack acknowledges that the message was processed successfully
func (m *Message[T]) Ack() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.processed {
		return fmt.Errorf("message already processed")
	}

	m.processed = true
	m.State = MessageStateCompleted
	m.UpdatedAt = time.Now()

	// Move from processing to completed directory
	return m.queue.completeMessage(context.Background(), m)
}

// Nack indicates that the message processing failed
func (m *Message[T]) Nack(err error) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.processed {
		return fmt.Errorf("message already processed")
	}

	m.processed = true
	m.State = MessageStateFailed
	if err != nil {
		m.Error = err.Error()
	}
	m.Retries++
	m.UpdatedAt = time.Now()

	// Handle message failure (potentially retry or move to DLQ)
	return m.queue.failMessage(context.Background(), m)
}

// QueueConfig holds configuration for filesystem queue
type QueueConfig struct {
	BasePath   string        // Base directory for queue files
	MaxRetries int           // Maximum number of retry attempts
	RetryDelay time.Duration // Delay between retries
}

// DefaultConfig returns a default queue configuration
func DefaultConfig() QueueConfig {
	return QueueConfig{
		BasePath:   "/tmp/fluxor/queue",
		MaxRetries: 3,
		RetryDelay: time.Second,
	}
}

// Queue implements a filesystem-based messaging.Queue
type Queue[T any] struct {
	fs            afs.Service
	config        QueueConfig
	pendingDir    string
	processingDir string
	completedDir  string
	failedDir     string
	dlqDir        string
	mu            sync.Mutex
}

// NewQueue creates a new filesystem-based queue
func NewQueue[T any](fs afs.Service, config QueueConfig) (*Queue[T], error) {
	if config.BasePath == "" {
		return nil, fmt.Errorf("base path cannot be empty")
	}

	q := &Queue[T]{
		fs:            fs,
		config:        config,
		pendingDir:    path.Join(config.BasePath, "pending"),
		processingDir: path.Join(config.BasePath, "processing"),
		completedDir:  path.Join(config.BasePath, "completed"),
		failedDir:     path.Join(config.BasePath, "failed"),
		dlqDir:        path.Join(config.BasePath, "dlq"),
	}

	// Ensure directories exist
	dirs := []string{
		q.pendingDir,
		q.processingDir,
		q.completedDir,
		q.failedDir,
		q.dlqDir,
	}

	ctx := context.Background()
	for _, dir := range dirs {
		exists, _ := fs.Exists(ctx, dir)
		if !exists {
			if err := fs.Create(ctx, dir, file.DefaultDirOsMode, true); err != nil {
				return nil, fmt.Errorf("failed to create directory %s: %w", dir, err)
			}
		}
	}

	return q, nil
}

// Publish adds a new message to the queue
func (q *Queue[T]) Publish(ctx context.Context, t *T) error {
	// Create a new message
	message := &Message[T]{
		ID:        uuid.New().String(),
		Data:      *t,
		State:     MessageStatePending,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		Retries:   0,
		queue:     q,
	}

	// Serialize message to JSON
	data, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	// Write to pending directory
	filename := q.generateFilename(message.ID)
	filePath := path.Join(q.pendingDir, filename)

	return q.uploadMessage(ctx, filePath, data)
}

// Consume retrieves and processes a message from the queue
func (q *Queue[T]) Consume(ctx context.Context) (messaging.Message[T], error) {
	// First, check if there are any failed messages to retry
	syncMessage, err := q.checkFailedMessages(ctx)
	if err != nil {
		return nil, err
	}
	if syncMessage != nil {
		return syncMessage, nil
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	// List pending messages
	objects, err := q.fs.List(ctx, q.pendingDir)
	if err != nil {
		return nil, fmt.Errorf("failed to list pending messages: %w", err)
	}

	// Filter only files, not directories
	var pendingFiles []storage.Object
	for _, obj := range objects {
		if !obj.IsDir() && strings.HasSuffix(obj.Name(), ".json") {
			pendingFiles = append(pendingFiles, obj)
		}
	}

	// If no pending messages, return nil
	if len(pendingFiles) == 0 {
		return nil, nil
	}

	// Process the oldest message (by filename prefix)
	obj := pendingFiles[0]

	// Read message content
	message, err := q.readMessageFromURL(ctx, obj.URL())
	if err != nil {
		// Move invalid message to failed directory
		destURL := path.Join(q.failedDir, fmt.Sprintf("invalid-%s", obj.Name()))
		_ = q.fs.Move(ctx, obj.URL(), destURL)
		return nil, err
	}

	// Update message state
	message.State = MessageStateProcessing
	message.UpdatedAt = time.Now()
	message.queue = q

	// Move to processing directory
	processingPath := path.Join(q.processingDir, obj.Name())

	// Serialize updated message
	updatedData, err := json.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal updated message: %w", err)
	}

	// Upload to processing directory first, then delete from pending
	if err := q.uploadMessage(ctx, processingPath, updatedData); err != nil {
		return nil, fmt.Errorf("failed to move message to processing directory: %w", err)
	}

	// Now it's safe to delete from pending
	if err := q.fs.Delete(ctx, obj.URL()); err != nil {
		return nil, fmt.Errorf("failed to delete message from pending directory: %w", err)
	}

	return message, nil
}

// checkFailedMessages looks for failed messages eligible for retry
func (q *Queue[T]) checkFailedMessages(ctx context.Context) (*Message[T], error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	// List failed messages
	objects, err := q.fs.List(ctx, q.failedDir, option.NewRecursive(false))
	if err != nil {
		return nil, fmt.Errorf("failed to list failed messages: %w", err)
	}

	// Filter for retry-eligible json files
	var failedFiles []storage.Object
	for _, obj := range objects {
		if !obj.IsDir() && strings.HasSuffix(obj.Name(), ".json") {
			failedFiles = append(failedFiles, obj)
		}
	}

	if len(failedFiles) == 0 {
		return nil, nil
	}

	// Process the oldest message
	obj := failedFiles[0]

	// Read message content
	message, err := q.readMessageFromURL(ctx, obj.URL())
	if err != nil {
		// Move invalid message to DLQ
		destURL := path.Join(q.dlqDir, fmt.Sprintf("invalid-%s", obj.Name()))
		_ = q.fs.Move(ctx, obj.URL(), destURL)
		return nil, err
	}

	// Check if retry limit exceeded - don't retry if already exceeded
	if message.Retries > q.config.MaxRetries {
		// Move to DLQ
		destURL := path.Join(q.dlqDir, obj.Name())
		if err := q.fs.Move(ctx, obj.URL(), destURL); err != nil {
			return nil, fmt.Errorf("failed to move message to DLQ: %w", err)
		}
		return nil, nil
	}

	// Update message for retry
	message.State = MessageStateProcessing
	message.UpdatedAt = time.Now()
	message.queue = q

	// Move to processing directory
	processingPath := path.Join(q.processingDir, obj.Name())

	// Serialize updated message
	updatedData, err := json.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal updated message: %w", err)
	}

	// Upload to processing directory first, then delete from failed
	if err := q.uploadMessage(ctx, processingPath, updatedData); err != nil {
		return nil, fmt.Errorf("failed to move message to processing directory: %w", err)
	}

	// Now it's safe to delete from failed
	if err := q.fs.Delete(ctx, obj.URL()); err != nil {
		return nil, fmt.Errorf("failed to delete message from failed directory: %w", err)
	}

	return message, nil
}

// completeMessage moves a message to the completed directory
func (q *Queue[T]) completeMessage(ctx context.Context, m *Message[T]) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Serialize message
	data, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("failed to marshal completed message: %w", err)
	}

	filename := q.generateFilename(m.ID)
	completedPath := path.Join(q.completedDir, filename)
	processingPath := path.Join(q.processingDir, filename)

	// Upload to completed directory
	if err := q.uploadMessage(ctx, completedPath, data); err != nil {
		return fmt.Errorf("failed to write message to completed directory: %w", err)
	}

	// Delete from processing directory
	if exists, _ := q.fs.Exists(ctx, processingPath); exists {
		if err := q.fs.Delete(ctx, processingPath); err != nil {
			return fmt.Errorf("failed to delete message from processing directory: %w", err)
		}
	}

	return nil
}

// failMessage handles a failed message (retry or move to DLQ)
func (q *Queue[T]) failMessage(ctx context.Context, m *Message[T]) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Serialize message
	data, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("failed to marshal failed message: %w", err)
	}

	filename := q.generateFilename(m.ID)
	processingPath := path.Join(q.processingDir, filename)

	// Check if retry limit exceeded
	if m.Retries > q.config.MaxRetries {
		// Move to DLQ
		dlqPath := path.Join(q.dlqDir, filename)
		if err := q.uploadMessage(ctx, dlqPath, data); err != nil {
			return fmt.Errorf("failed to write message to DLQ: %w", err)
		}
	} else {
		// Move back to failed directory for retry
		failedPath := path.Join(q.failedDir, filename)
		if err := q.uploadMessage(ctx, failedPath, data); err != nil {
			return fmt.Errorf("failed to write message to failed directory: %w", err)
		}
	}

	// Delete from processing directory
	if exists, _ := q.fs.Exists(ctx, processingPath); exists {
		if err := q.fs.Delete(ctx, processingPath); err != nil {
			return fmt.Errorf("failed to delete message from processing directory: %w", err)
		}
	}

	return nil
}

// Helper methods to abstract common operations

// generateFilename generates a consistent filename for a message
func (q *Queue[T]) generateFilename(id string) string {
	return fmt.Sprintf("%s.json", id)
}

// uploadMessage abstracts the common operation of uploading message data
func (q *Queue[T]) uploadMessage(ctx context.Context, path string, data []byte) error {
	return q.fs.Upload(ctx, path, file.DefaultFileOsMode, bytes.NewBuffer(data))
}

// readMessageFromURL abstracts the common operation of reading and unmarshaling a message
func (q *Queue[T]) readMessageFromURL(ctx context.Context, url string) (*Message[T], error) {
	data, err := q.fs.DownloadWithURL(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("failed to read message %s: %w", url, err)
	}

	var message Message[T]
	if err := json.Unmarshal(data, &message); err != nil {
		return nil, fmt.Errorf("failed to unmarshal message %s: %w", url, err)
	}

	return &message, nil
}

// ensure Queue implements messaging.Queue interface
var _ messaging.Queue[any] = (*Queue[any])(nil)
