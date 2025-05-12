package event

import (
	"context"
	"log"
)

type Listener[T any] struct {
	publisher *Publisher[T]
	handler   func(*Event[T])
	ctx       context.Context
}

func NewListener[T any](publisher *Publisher[T], handler func(*Event[T])) *Listener[T] {
	return &Listener[T]{
		publisher: publisher,
		handler:   handler,
		ctx:       context.Background(),
	}
}
func (l *Listener[T]) Stop() {
	l.ctx.Done()
}

func (l *Listener[T]) Start() {
	go func() {
		for {
			select {
			case <-l.ctx.Done():
				return
			default:
				event, err := l.publisher.Consume(l.ctx)
				if err != nil {
					log.Printf("Error consuming event: %v", err)
				}
				l.handler(event)
			}
		}
	}()
}
