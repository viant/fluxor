package state

import (
	"github.com/viant/bindly/state"
)

// Parameter represents a named value
type Parameter struct {
	Name     string          `json:"name" yaml:"name"`
	Value    interface{}     `json:"value" yaml:"value"`
	DataType string          `json:"dataType,omitempty" yaml:"dataType,omitempty"`
	Location *state.Location `json:"location,omitempty" yaml:"location,omitempty"`
	Default  interface{}     `json:"default,omitempty" yaml:"default,omitempty"`
}

// Parameters is a collection of named values
type Parameters []*Parameter

// Add appends a parameter to the collection
func (p *Parameters) Add(name string, value interface{}) {
	*p = append(*p, &Parameter{
		Name:  name,
		Value: value,
	})
}

// Get retrieves a parameter by name
func (p Parameters) Get(name string) (*Parameter, bool) {
	for _, param := range p {
		if param.Name == name {
			return param, true
		}
	}
	return nil, false
}

// ToMap converts Parameters to a map
func (p Parameters) ToMap() map[string]interface{} {
	result := make(map[string]interface{})
	for _, param := range p {
		result[param.Name] = param.Value
	}
	return result
}

// FromMap creates Parameters from a map
func FromMap(m map[string]interface{}) Parameters {
	params := make(Parameters, 0, len(m))
	for k, v := range m {
		params = append(params, &Parameter{
			Name:  k,
			Value: v,
		})
	}
	return params
}
