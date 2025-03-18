package memory

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type TestPayload struct {
	ID      string
	Message string
	Count   int
}

func TestQueue(t *testing.T) {
	// Create queue with default config
	config := DefaultConfig()
	config.RetryDelay = 10 * time.Millisecond // Speed up for testing
	queue := NewQueue[TestPayload](config)

	// Test publishing and consuming
	ctx := context.Background()
	payload := TestPayload{
		ID:      "test-1",
		Message: "Hello, world!",
		Count:   1,
	}

	// Publish a message
	err := queue.Publish(ctx, &payload)
	assert.NoError(t, err)
	assert.Equal(t, 1, queue.Size())

	// Consume the message
	message, err := queue.Consume(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, message)
	assert.Equal(t, 0, queue.Size())

	// Verify the message content
	msgData := message.T()
	assert.Equal(t, payload.ID, msgData.ID)
	assert.Equal(t, payload.Message, msgData.Message)
	assert.Equal(t, payload.Count, msgData.Count)

	// Test acknowledgment
	err = message.Ack()
	assert.NoError(t, err)

	// Make sure ack is processed
	time.Sleep(20 * time.Millisecond)

	// Test double ack (should error)
	err = message.Ack()
	assert.Error(t, err)
}

func TestQueueRetries(t *testing.T) {
	// Create queue with test config
	config := DefaultConfig()
	config.MaxRetries = 2
	config.RetryDelay = 10 * time.Millisecond // Speed up for testing
	queue := NewQueue[TestPayload](config)

	ctx := context.Background()
	payload := TestPayload{
		ID:      "retry-test",
		Message: "Test retries",
		Count:   1,
	}

	// Publish a message
	err := queue.Publish(ctx, &payload)
	assert.NoError(t, err)

	// First attempt
	message, err := queue.Consume(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, message)

	// Nack to trigger retry
	err = message.Nack(nil)
	assert.NoError(t, err)

	// Wait for retry delay
	time.Sleep(20 * time.Millisecond)

	// Second attempt
	message, err = queue.Consume(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, message)

	// Nack again
	err = message.Nack(nil)
	assert.NoError(t, err)

	// Wait for retry delay
	time.Sleep(20 * time.Millisecond)

	// Third attempt (final)
	message, err = queue.Consume(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, message)

	// Nack one more time (should exceed max retries)
	err = message.Nack(nil)
	assert.NoError(t, err)

	// Wait for processing
	time.Sleep(20 * time.Millisecond)

	// No more retries, queue should be empty
	assert.Equal(t, 0, queue.Size())
}

func TestQueueConcurrency(t *testing.T) {
	// Create queue
	config := DefaultConfig()
	config.RetryDelay = 10 * time.Millisecond
	queue := NewQueue[TestPayload](config)

	ctx := context.Background()
	concurrency := 10
	messagesPerProducer := 10

	// Use WaitGroup to coordinate test completion
	var wg sync.WaitGroup
	wg.Add(concurrency * 2) // producers + consumers

	// Track consumed messages
	var consumedCount int
	var consumedMu sync.Mutex

	// Start consumers
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()

			for j := 0; j < messagesPerProducer; j++ {
				message, err := queue.Consume(ctx)
				if err != nil {
					t.Errorf("Error consuming: %v", err)
					continue
				}

				if message == nil {
					// No message available, wait a bit
					time.Sleep(10 * time.Millisecond)
					j--
					continue
				}

				// Process the message
				err = message.Ack()
				assert.NoError(t, err)

				// Count consumed messages
				consumedMu.Lock()
				consumedCount++
				consumedMu.Unlock()
			}
		}()
	}

	// Start producers
	for i := 0; i < concurrency; i++ {
		go func(producerID int) {
			defer wg.Done()

			for j := 0; j < messagesPerProducer; j++ {
				payload := TestPayload{
					ID:      fmt.Sprintf("p%d-m%d", producerID, j),
					Message: fmt.Sprintf("Message %d from producer %d", j, producerID),
					Count:   j,
				}

				err := queue.Publish(ctx, &payload)
				if err != nil {
					t.Errorf("Error publishing: %v", err)
				}

				// Small delay to avoid overwhelming
				time.Sleep(1 * time.Millisecond)
			}
		}(i)
	}

	// Wait for completion with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("Test timed out")
	}

	// Verify all messages were consumed
	assert.Equal(t, concurrency*messagesPerProducer, consumedCount)
	assert.Equal(t, 0, queue.Size())
}

func TestQueueContextCancellation(t *testing.T) {
	queue := NewQueue[TestPayload](DefaultConfig())

	// Create a context that we'll cancel
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel the context immediately
	cancel()

	// Try to publish with cancelled context
	payload := TestPayload{ID: "test"}
	err := queue.Publish(ctx, &payload)
	assert.Error(t, err)

	// Set up a scenario where Consume would block
	emptyCtx := context.Background()

	// Create a goroutine to cancel after a delay
	ctxWithTimeout, cancelTimeout := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelTimeout()

	// Consume should return with an error when context is done
	_, err = queue.Consume(ctxWithTimeout)
	assert.Error(t, err)

	// Ensure queue is still usable after context cancellation
	err = queue.Publish(emptyCtx, &payload)
	assert.NoError(t, err)

	message, err := queue.Consume(emptyCtx)
	assert.NoError(t, err)
	assert.NotNil(t, message)
}
