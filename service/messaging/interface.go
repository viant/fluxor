package messaging

import (
	"context"
)

// Vendor represents the name of a messaging vendor
type Vendor string

// Queue represents an abstract message queue for any payload type
type Queue[T any] interface {
	// Publish adds a new message with payload to the queue
	Publish(ctx context.Context, t *T) error

	// Consume retrieves a single message from the queue
	Consume(ctx context.Context) (Message[T], error)
}

// Message represents a message retrieved from a queue
type Message[T any] interface {
	// T returns the payload of this message
	T() *T

	// Ack acknowledges successful processing of this message
	Ack() error

	// Nack indicates failure in processing this message
	Nack(err error) error
}

// QueueConfig defines standard configuration options for queue implementations
type QueueConfig struct {
	// MaxRetries specifies how many times a message can be retried
	MaxRetries int

	// RetryDelay specifies the time to wait before retrying a failed message
	RetryDelay int

	// VisibilityTimeout specifies how long a message is considered in-flight
	VisibilityTimeout int

	// AdditionalConfig allows implementation-specific configurations
	AdditionalConfig map[string]interface{}
}
