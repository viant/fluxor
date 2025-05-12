package event

import (
	"context"
	"github.com/viant/fluxor/service/messaging"
	"time"
)

type Publisher[T any] struct {
	queue    messaging.Queue[Event[T]]
	anyQueue messaging.Queue[Event[any]]
}

func NewPublisher[T any](queue messaging.Queue[Event[T]]) *Publisher[T] {
	return &Publisher[T]{
		queue: queue,
	}
}

func (p *Publisher[T]) Publish(ctx context.Context, event *Event[T]) error {
	event.CreatedAt = time.Now()
	if p.anyQueue != nil {
		p.anyQueue.Publish(ctx, &Event[any]{
			Context:   event.Context,
			CreatedAt: event.CreatedAt,
			Metadata:  event.Metadata,
			Data:      event.Data,
		})
	}
	return p.queue.Publish(ctx, event)
}

func (p *Publisher[T]) Consume(ctx context.Context) (*Event[T], error) {
	msg, err := p.queue.Consume(ctx)
	if err != nil || msg == nil {
		return nil, err
	}
	if err = msg.Ack(); err != nil {
		return nil, err
	}
	return msg.T(), nil
}
