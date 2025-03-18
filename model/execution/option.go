package execution

import (
	"github.com/viant/fluxor/extension"
	"github.com/viant/fluxor/model"
	"github.com/viant/structology/conv"
)

type Option func(session *Session)

// WithState sets the state for the session
func WithImports(imports ...*model.Import) Option {
	return func(session *Session) {
		session.imports = append(session.imports, imports...)
	}
}

// WithState sets the state for the session
func WithTypes(types *extension.Types) Option {
	return func(session *Session) {
		session.types = types
	}
}

// WithState sets the state for the session
func WithConverter(converter *conv.Converter) Option {
	return func(session *Session) {
		session.converter = converter
	}
}

func WithState(state map[string]interface{}) Option {
	return func(session *Session) {
		for k, v := range state {
			session.State[k] = v
		}
	}
}
