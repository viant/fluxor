package extension

import (
	"github.com/viant/fluxor/model/types"
	"github.com/viant/x"
	"sync"
)

// Actions provides action service
type Actions struct {
	types    *Types
	services map[string]types.Service
	mux      sync.RWMutex
}

func (s *Actions) Types() *Types {
	return s.types
}

// Lookup returns a service by name
func (s *Actions) Lookup(name string) types.Service {
	s.mux.RLock()
	defer s.mux.RUnlock()
	return s.services[name]
}

// Register registers a service
func (s *Actions) Register(service types.Service) {
	s.mux.Lock()
	defer s.mux.Unlock()

	if typer, ok := service.(DataTypeIniter); ok {
		typer.InitTypes(s.types)
	}
	s.services[service.Name()] = service
}

// NewActions creates a new action service
func NewActions(goTypes ...*x.Type) *Actions {
	ret := &Actions{
		types:    NewTypes(),
		services: make(map[string]types.Service),
	}
	for _, t := range goTypes {
		if t != nil {
			ret.types.Register(t)
		}
	}
	return ret
}
