package extension

import "github.com/viant/fluxor/model"

type Option func(*Types)

func WithImports(imports model.Imports) Option {
	return func(t *Types) {
		t.imports = imports
	}
}
