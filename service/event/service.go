package event

import (
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/fluxor/service/messaging"
	"github.com/viant/fluxor/service/messaging/fs"
	"github.com/viant/fluxor/service/messaging/memory"
	"reflect"
	"sync"
)

type Service struct {
	publisher         *Publisher[any]
	listener          *Listener[any]
	typedPublishers   map[reflect.Type]any
	typedListener     map[reflect.Type]any
	mux               *sync.RWMutex
	queueVendor       messaging.Vendor
	fsNewQueueConfig  func(name string) fs.Config
	memNewQueueConfig func(name string) memory.Config
}

func (s *Service) SetListener(handler func(*Event[any])) {
	if s.listener != nil {
		s.listener.Stop()
	}
	s.listener = NewListener[any](s.publisher, handler)
	s.listener.Start()
	return
}

func New(queueVendor messaging.Vendor, opts ...Option) (*Service, error) {
	ret := &Service{
		queueVendor:     queueVendor,
		typedPublishers: make(map[reflect.Type]any),
		typedListener:   make(map[reflect.Type]any),
		mux:             &sync.RWMutex{},
	}
	for _, opt := range opts {
		opt(ret)
	}

	switch queueVendor {
	case "fs":
		if ret.fsNewQueueConfig == nil {
			return nil, fmt.Errorf("fs queue vendor requires fsNewQueueConfig")
		}
	case "memory":
		if ret.memNewQueueConfig == nil {
			return nil, fmt.Errorf("memory queue vendor requires memNewQueueConfig")
		}
	default:
		return nil, fmt.Errorf("unsupported queue vendor: %s", queueVendor)
	}

	queue, err := QueueOf[Event[any]](ret, "any")
	if err != nil {
		return nil, err
	}
	ret.publisher = NewPublisher[any](queue)
	return ret, nil
}

func QueueOf[T any](s *Service, name string) (messaging.Queue[T], error) {
	switch s.queueVendor {
	case "fs":
		return fs.NewQueue[T](afs.New(), s.fsNewQueueConfig(name))
	case "memory":
		return memory.NewQueue[T](s.memNewQueueConfig(name)), nil
	}
	return nil, fmt.Errorf("unsupported queue vendor: %s", s.queueVendor)
}

func keyOf[T any]() reflect.Type {
	var t T
	rType := reflect.TypeOf(t)
	if rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}
	return rType
}

func SetListenerOf[T any](s *Service, handler func(*Event[T])) error {
	key := keyOf[T]()
	s.mux.RLock()
	ret, ok := s.typedListener[key]
	s.mux.RUnlock()
	if ok {
		ret.(*Listener[T]).Stop()
	}
	publisher, err := PublisherOf[T](s)
	if err != nil {
		return err
	}
	listener := NewListener[T](publisher, handler)
	s.mux.Lock()
	s.typedListener[key] = listener
	listener.Start()
	s.mux.Unlock()
	return nil
}

// PublisherOf returns a publisher for the provided type
func PublisherOf[T any](s *Service) (*Publisher[T], error) {
	key := keyOf[T]()
	s.mux.RLock()
	ret, ok := s.typedPublishers[key]
	s.mux.RUnlock()
	if !ok {
		queue, err := QueueOf[Event[T]](s, key.String())
		if err != nil {
			return nil, err
		}
		publisher := NewPublisher[T](queue)
		publisher.anyQueue = s.publisher.queue
		s.mux.Lock()
		s.typedPublishers[key] = publisher
		s.mux.Unlock()
		return publisher, nil
	}
	return ret.(*Publisher[T]), nil
}
