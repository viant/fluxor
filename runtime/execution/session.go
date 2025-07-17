package execution

import (
	"fmt"
	"github.com/viant/fluxor/extension"
	"github.com/viant/fluxor/model"
	"github.com/viant/fluxor/model/state"
	"github.com/viant/fluxor/runtime/expander"
	"github.com/viant/structology/conv"
	"reflect"
	"sync"
)

// Session represents the execution context for a process
type Session struct {
	ID        string
	State     map[string]interface{}
	Context   map[string]interface{}
	types     *extension.Types
	imports   model.Imports
	converter *conv.Converter
	mu        sync.RWMutex
	listeners []StateListener // invoked on Set
	whenL     []WhenListener  // invoked on when-condition evaluation
}

// WhenListener is invoked every time a `when:` expression is evaluated. The
// listener receives the session (at evaluation time), the raw expression and
// the boolean outcome of the evaluation.
type WhenListener func(s *Session, expr string, result bool)

// StateListener is invoked every time Session.Set overwrites an existing key
// or inserts a new one.
type StateListener func(s *Session, key string, oldVal, newVal interface{})

// RegisterListeners attaches a callback that will be called on every Set.
// The call is made synchronously while the session mutex is held, therefore
// listeners MUST return quickly and must not call back into Session to avoid
// deadlocks.
func (s *Session) RegisterListeners(fn ...StateListener) {
	if fn == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listeners = append(s.listeners, fn...)
}

// RegisterWhenListeners attaches callbacks that are executed after every
// `when:` condition evaluation.
func (s *Session) RegisterWhenListeners(fn ...WhenListener) {
	if fn == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.whenL = append(s.whenL, fn...)
}

// FireWhen notifies all registered when-listeners. It is exported so that code
// outside the execution package (e.g. allocator) can emit the event.
func (s *Session) FireWhen(expr string, result bool) {
	s.mu.RLock()
	lst := append([]WhenListener(nil), s.whenL...)
	s.mu.RUnlock()
	for _, fn := range lst {
		fn(s, expr, result)
	}
}

// Set adds or updates a parameter in the session
func (s *Session) Set(key string, value interface{}) {
	s.mu.Lock()
	old := s.State[key]
	s.State[key] = value
	s.mu.Unlock()

	for _, fn := range s.listeners {
		fn(s, key, old, value)
	}
}

// Get retrieves a parameter from the session
func (s *Session) Get(key string) (interface{}, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, exists := s.State[key]
	return value, exists
}

func (s *Session) Append(key string, value interface{}) {
	if value == nil { // nothing to add
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	// Ensure we have a destination slice ([]interface{}).
	var dst []interface{}
	if cur, ok := s.State[key]; ok && cur != nil {
		switch v := cur.(type) {
		case []interface{}:
			dst = v
		default:
			dst = []interface{}{v}
		}
	}

	// Helper to append one element.
	add := func(elem interface{}) {
		if elem != nil {
			dst = append(dst, elem)
		}
	}

	// If the incoming value is a slice/array, append its elements.
	rv := reflect.ValueOf(value)
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		if rv.Len() == 0 { // empty slice ⇒ nothing to add
			return
		}
		for i := 0; i < rv.Len(); i++ {
			add(rv.Index(i).Interface())
		}
	} else { // single element
		add(value)
	}

	s.State[key] = dst
}

func (s *Session) TaskSession(from map[string]interface{}, options ...Option) *Session {
	ret := NewSession(s.ID, options...)

	if len(s.listeners) > 0 {
		ret.listeners = s.listeners
	}
	if len(s.whenL) > 0 {
		ret.whenL = s.whenL
	}

	for k, v := range from {
		ret.State[k] = v
	}
	for k, v := range s.State {
		if _, ok := ret.State[k]; ok {
			continue
		}
		ret.State[k] = v
	}
	return ret
}

// GetString retrieves a parameter as a string
func (s *Session) GetString(key string) (string, bool) {
	value, exists := s.Get(key)
	if !exists {
		return "", false
	}

	strVal, ok := value.(string)
	return strVal, ok
}

// GetInt retrieves a parameter as an integer
func (s *Session) GetInt(key string) (int, bool) {
	value, exists := s.Get(key)
	if !exists {
		return 0, false
	}

	intVal, ok := value.(int)
	return intVal, ok
}

// GetBool retrieves a parameter as a boolean
func (s *Session) GetBool(key string) (bool, bool) {
	value, exists := s.Get(key)
	if !exists {
		return false, false
	}

	boolVal, ok := value.(bool)
	return boolVal, ok
}

// Expand expands a value using the session state
func (s *Session) Expand(value interface{}) (interface{}, error) {
	return expander.Expand(value, s.State)
}

// ApplyParameters applies a list of parameters to the session
func (s *Session) ApplyParameters(params state.Parameters) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var err error
	for _, param := range params {
		value := param.Value
		if value, err = expander.Expand(param.Value, s.State); err != nil {
			return err
		}
		value, err = s.ensureValueType(param.DataType, value)
		if err != nil {
			return err
		}
		s.State[param.Name] = value
	}
	return nil
}

// Clone creates a copy of the session
func (s *Session) Clone() *Session {
	s.mu.RLock()
	defer s.mu.RUnlock()

	clone := NewSession(s.ID)
	clone.listeners = append(clone.listeners, s.listeners...)
	clone.whenL = append(clone.whenL, s.whenL...)
	for k, v := range s.State {
		clone.State[k] = v
	}
	return clone
}

// GetAll returns all parameters in the session
func (s *Session) GetAll() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Make a copy to avoid concurrent access issues
	result := make(map[string]interface{}, len(s.State))
	for k, v := range s.State {
		result[k] = v
	}

	return result
}

func (s *Session) ensureValueType(dataType string, value interface{}) (interface{}, error) {
	if dataType == "" {
		return value, nil
	}
	if s.types == nil {
		return nil, fmt.Errorf("types not initialized")
	}
	if s.imports == nil {
		return nil, fmt.Errorf("imports not initialized")
	}

	aType := s.types.Lookup(dataType, extension.WithImports(s.imports))
	if aType == nil {
		return nil, fmt.Errorf("type %v not registered", dataType)
	}

	return s.TypedValue(aType.Type, value)
}

// TypedValue converts a value to the specified type
func (s *Session) TypedValue(aType reflect.Type, value interface{}) (interface{}, error) {
	if s.converter == nil {
		s.converter = conv.NewConverter(conv.DefaultOptions())
	}
	instance := newInstancePtr(aType)
	err := s.converter.Convert(value, instance)
	if aType.Kind() == reflect.Slice {
		instance = reflect.ValueOf(instance).Elem().Interface()
	}
	return instance, err
}

// NewSession creates a new session
func NewSession(id string, opt ...Option) *Session {
	ret := &Session{
		ID:      id,
		State:   make(map[string]interface{}),
		Context: make(map[string]interface{}),
	}

	for _, o := range opt {
		o(ret)
	}
	if len(ret.imports) == 0 && ret.types != nil {
		ret.imports = ret.types.Imports()
	}

	return ret
}

var empty interface{}

// newInstancePtr creates a new instance pointer of the given type
func newInstancePtr(t reflect.Type) interface{} {
	if t == nil {
		return empty
	}

	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return reflect.New(t).Interface()
}
