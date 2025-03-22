package memory

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/viant/fluxor/service/messaging"
	"sync"
	"time"
)

// Config for memory queue implementation
type Config struct {
	MaxRetries  int
	RetryDelay  time.Duration
	DeadLetter  bool
	QueueBuffer int
}

// DefaultConfig returns a standard configuration for memory queue
func DefaultConfig() Config {
	return Config{
		MaxRetries:  3,
		RetryDelay:  100 * time.Millisecond,
		DeadLetter:  true,
		QueueBuffer: 100,
	}
}

// Message implements mbus.Message interface for in-memory queue
type Message[T any] struct {
	id         string
	payload    T
	queue      *Queue[T]
	retryCount int
	mu         sync.Mutex
	processed  bool
	createdAt  time.Time
}

// T returns the message payload
func (m *Message[T]) T() *T {
	return &m.payload
}

// Ack acknowledges the message as processed successfully
func (m *Message[T]) Ack() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.processed {
		return fmt.Errorf("message already processed")
	}

	m.processed = true
	return nil
}

// Nack indicates a failure in processing the message
func (m *Message[T]) Nack(err error) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.processed {
		return fmt.Errorf("message already processed")
	}

	m.processed = true
	m.retryCount++

	// If under retry limit, requeue
	if m.retryCount <= m.queue.config.MaxRetries {
		go func() {
			// WaitForProcess for retry delay
			time.Sleep(m.queue.config.RetryDelay)

			// Create a new message with incremented retry count
			newMsg := &Message[T]{
				id:         m.id,
				payload:    m.payload,
				queue:      m.queue,
				retryCount: m.retryCount,
				createdAt:  time.Now(),
			}

			// Add back to the queue
			m.queue.mu.Lock()
			m.queue.messages <- newMsg
			m.queue.mu.Unlock()
		}()
	} else if m.queue.config.DeadLetter {
		// Move to dead letter queue
		m.queue.dlqMu.Lock()
		m.queue.dlq = append(m.queue.dlq, m)
		m.queue.dlqMu.Unlock()
	}

	return nil
}

// Queue implements an in-memory messaging.Queue
type Queue[T any] struct {
	messages chan *Message[T]
	dlq      []*Message[T]
	config   Config
	mu       sync.Mutex
	dlqMu    sync.Mutex
}

// NewQueue creates a new in-memory queue
func NewQueue[T any](config Config) *Queue[T] {
	if config.QueueBuffer <= 0 {
		config.QueueBuffer = DefaultConfig().QueueBuffer
	}

	return &Queue[T]{
		messages: make(chan *Message[T], config.QueueBuffer),
		dlq:      make([]*Message[T], 0),
		config:   config,
	}
}

// Publish adds a new item to the queue
func (q *Queue[T]) Publish(ctx context.Context, t *T) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		msg := &Message[T]{
			id:         uuid.New().String(),
			payload:    *t,
			queue:      q,
			retryCount: 0,
			createdAt:  time.Now(),
		}

		select {
		case q.messages <- msg:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Consume retrieves a single item from the queue
func (q *Queue[T]) Consume(ctx context.Context) (messaging.Message[T], error) {
	select {
	case msg := <-q.messages:
		return msg, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Size returns the current number of messages in the queue
func (q *Queue[T]) Size() int {
	return len(q.messages)
}

// DLQSize returns the number of messages in the dead letter queue
func (q *Queue[T]) DLQSize() int {
	q.dlqMu.Lock()
	defer q.dlqMu.Unlock()
	return len(q.dlq)
}

// ensure Queue implements messaging.Queue interface
var _ messaging.Queue[any] = (*Queue[any])(nil)
