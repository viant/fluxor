package extension

import (
	"fmt"
	"github.com/viant/fluxor/model/types"
	"github.com/viant/x"
	"reflect"
	"sync"
)

// Actions provides action service
type Actions struct {
	types    *Types
	services map[string]types.Service
	byType   map[reflect.Type]types.Service
	mux      sync.RWMutex
}

func (s *Actions) Services() []string {
	s.mux.RLock()
	defer s.mux.RUnlock()
	var services []string
	for service := range s.services {
		services = append(services, service)
	}
	return services
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
func (s *Actions) Register(service types.Service) error {
	s.mux.Lock()
	defer s.mux.Unlock()
	_, ok := s.services[service.Name()]
	if ok {
		return fmt.Errorf("service %v already registered", service.Name())
	}
	if typer, ok := service.(DataTypeIniter); ok {
		typer.InitTypes(s.types)
	}
	s.services[service.Name()] = service
	rType := TypeServiceOf(service)
	s.byType[rType] = service
	return nil
}

func TypeServiceOf(service types.Service) reflect.Type {
	rType := reflect.TypeOf(service)
	if rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}
	return rType
}

func LookupService[T types.Service](actions *Actions) T {
	var t T
	key := TypeServiceOf(t)
	actions.mux.RLock()
	service := actions.byType[key]
	actions.mux.RUnlock()
	return service.(T)
}

// NewActions creates a new action service
func NewActions(goTypes ...*x.Type) *Actions {
	ret := &Actions{
		types:    NewTypes(),
		byType:   make(map[reflect.Type]types.Service),
		services: make(map[string]types.Service),
	}
	for _, t := range goTypes {
		if t != nil {
			ret.types.Register(t)
		}
	}
	return ret
}
