package fs

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/afs"
	"os"
	"path"
	"testing"
	"time"
)

type TestPayload struct {
	ID      string `json:"id"`
	Message string `json:"message"`
	Count   int    `json:"count"`
}

func TestQueue(t *testing.T) {
	// Create temp directory for queue
	tempDir, err := os.MkdirTemp("/tmp", "queue-test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Initialize filesystem
	fs := afs.New()
	ctx := context.Background()

	// Configure queue
	config := QueueConfig{
		BasePath:   tempDir,
		MaxRetries: 2,
		RetryDelay: 10 * time.Millisecond,
	}

	// Create queue
	queue, err := NewQueue[TestPayload](fs, config)
	assert.NoError(t, err)
	assert.NotNil(t, queue)

	// Test directory structure
	dirs := []string{
		queue.pendingDir,
		queue.processingDir,
		queue.completedDir,
		queue.failedDir,
		queue.dlqDir,
	}

	for _, dir := range dirs {
		exists, err := fs.Exists(ctx, dir)
		assert.NoError(t, err)
		assert.True(t, exists, fmt.Sprintf("Directory %s should exist", dir))
	}

	// Test publishing messages
	testCases := []TestPayload{
		{ID: "1", Message: "Test message 1", Count: 1},
		{ID: "2", Message: "Test message 2", Count: 2},
		{ID: "3", Message: "Test message 3", Count: 3},
	}

	for _, payload := range testCases {
		err := queue.Publish(ctx, &payload)
		assert.NoError(t, err)
	}

	// Check pending directory
	objects, err := fs.List(ctx, queue.pendingDir)

	assert.NoError(t, err)
	assert.Equal(t, 3, len(objects)-1, "Should have 3 files in pending directory")

	// Test consuming messages
	for i := 0; i < len(testCases); i++ {
		// Consume message
		message, err := queue.Consume(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, message)

		// Verify payload data
		payload := message.T()
		assert.NotNil(t, payload)
		assert.Contains(t, []string{"1", "2", "3"}, payload.ID)

		// Test acknowledgment
		err = message.Ack()
		assert.NoError(t, err)

		// Verify message moved to completed
		time.Sleep(10 * time.Millisecond)
		completedObjects, err := fs.List(ctx, queue.completedDir)
		assert.NoError(t, err)
		assert.Equal(t, i+1, len(completedObjects)-1, "Should have completed objects")
	}

	// Test failure and retry
	payload := TestPayload{ID: "4", Message: "Failure test", Count: 4}
	err = queue.Publish(ctx, &payload)
	assert.NoError(t, err)

	// Consume and nack the message
	message, err := queue.Consume(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, message)

	// Nack should move to failed
	err = message.Nack(nil)
	assert.NoError(t, err)

	// Verify message moved to failed
	time.Sleep(10 * time.Millisecond)
	failedObjects, err := fs.List(ctx, queue.failedDir)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(failedObjects)-1, "Should have one file in failed directory")

	// Consume again, should get the failed message
	message, err = queue.Consume(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, message)

	// Nack again, now the retry count is 2
	err = message.Nack(nil)
	assert.NoError(t, err)

	// Consume again, should get the failed message again
	message, err = queue.Consume(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, message)

	// Nack one more time, now retry count exceeds max (2)
	err = message.Nack(nil)
	assert.NoError(t, err)

	// Verify message moved to DLQ
	time.Sleep(10 * time.Millisecond)
	dlqObjects, err := fs.List(ctx, queue.dlqDir)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(dlqObjects)-1, "Should have one file in DLQ directory")

	// Ensure no more messages
	message, err = queue.Consume(ctx)
	assert.NoError(t, err)
	assert.Nil(t, message, "Should have no more messages to consume")
}

func TestQueueInitialization(t *testing.T) {
	// Test with invalid config
	fs := afs.New()
	_, err := NewQueue[TestPayload](fs, QueueConfig{})
	assert.Error(t, err, "Should error with empty BasePath")

	// Test with non-existent directory
	tempDir := path.Join(os.TempDir(), fmt.Sprintf("queue-init-test-%d", time.Now().UnixNano()))
	config := QueueConfig{
		BasePath:   tempDir,
		MaxRetries: 2,
	}

	queue, err := NewQueue[TestPayload](fs, config)
	assert.NoError(t, err)
	assert.NotNil(t, queue)

	// Clean up
	os.RemoveAll(tempDir)
}
