package extension

import (
	"fmt"
	"github.com/viant/fluxor/model"
	"github.com/viant/x"
	"reflect"
	"strings"
)

type Types struct {
	x.Registry
	imports model.Imports
}

// Register adds a data type to the registry
func (t *Types) Register(dataType *x.Type) {
	if dataType.PkgPath != "" {
		if idx := strings.LastIndex(dataType.PkgPath, "/"); idx > 0 {
			pkgPath := dataType.PkgPath[:idx]
			if !t.imports.HasPkgPath(pkgPath) {
				t.imports = append(t.imports, &model.Import{Package: dataType.PkgPath[idx+1:], PkgPath: dataType.PkgPath})
			}
		}
	}
	t.Registry.Register(dataType)
}

// Lookup returns a data type from the registry
func (t *Types) Lookup(dataType string, options ...Option) *x.Type {
	temp := &Types{imports: t.imports}
	for _, opt := range options {
		opt(temp)
	}

	typeModifier := ""
	if idx := strings.LastIndex(dataType, "]"); idx != -1 {
		typeModifier = dataType[:idx+1]
		dataType = dataType[idx+1:]
	}

	if idx := strings.LastIndex(dataType, "."); idx != -1 {
		var pkg, typeName string
		pkg = dataType[:idx]
		typeName = dataType[idx+1:]
		if pkgPath := temp.imports.PkgPath(pkg); pkgPath != "" {
			pkg = pkgPath
		}
		dataType = fmt.Sprintf("%s.%s", pkg, typeName)
	}
	ret := t.Registry.Lookup(dataType)
	rType := ret.Type

	switch strings.TrimSpace(typeModifier) {
	case "[]":
		rType = reflect.SliceOf(rType)
	case "[][]":
		rType = reflect.SliceOf(reflect.SliceOf(rType))
	case "map[string]":
		rType = reflect.MapOf(reflect.TypeOf(""), rType)
	case "map[string][]":
		rType = reflect.MapOf(reflect.TypeOf(""), reflect.SliceOf(rType))
	}
	if rType != ret.Type {
		return x.NewType(rType)
	}
	return ret
}

// Imports returns import
func (t *Types) Imports() model.Imports {
	return t.imports
}

// NewTypes creates a new types
func NewTypes(options ...x.RegistryOption) *Types {
	result := &Types{
		Registry: *x.NewRegistry(options...),
	}
	return result
}
