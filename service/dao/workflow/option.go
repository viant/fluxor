package workflow

import "github.com/viant/fluxor/service/meta"

type Option func(*Service)

// WithRootTaskNodeName sets the root task node name
func WithRootTaskNodeName(name string) Option {
	return func(s *Service) {
		s.rootTaskNodeName = name
	}
}

// WithMetaService sets the meta service
func WithMetaService(meta *meta.Service) Option {
	return func(s *Service) {
		s.metaService = meta
	}
}
