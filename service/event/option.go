package event

import (
	"github.com/viant/fluxor/service/messaging/fs"
	"github.com/viant/fluxor/service/messaging/memory"
)

type Option func(s *Service)

// WithNewFsQueueConfig sets the new file system queue configuration
func WithNewFsQueueConfig(newConfig func(name string) fs.Config) Option {
	return func(s *Service) {
		s.fsNewQueueConfig = newConfig
	}
}

// WithNewMemoryQueueConfig  sets the new memory queue configuration
func WithNewMemoryQueueConfig(newQueue func(name string) memory.Config) Option {
	return func(s *Service) {
		s.memNewQueueConfig = newQueue
	}
}
