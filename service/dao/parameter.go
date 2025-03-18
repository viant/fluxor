package dao

type Parameter struct {
	Name  string
	Value interface{}
}

func NewParameter(name string, values ...string) *Parameter {
	if len(values) == 1 {
		return &Parameter{Name: name, Value: values[0]}
	}
	return &Parameter{Name: name, Value: values}
}
