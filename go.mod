module github.com/viant/fluxor

go 1.23.1

toolchain go1.23.7

require (
	github.com/google/uuid v1.6.0
	github.com/stretchr/testify v1.10.0
	github.com/viant/afs v1.25.1
	github.com/viant/bindly v0.0.0-20250313144058-d659e5bf9203
	github.com/viant/gosh v0.0.0-20240315215121-a5efb9835616
	github.com/viant/parsly v0.3.3
	github.com/viant/scy v0.15.4
	github.com/viant/structology v0.7.0
	github.com/viant/x v0.3.0
	github.com/zeebo/assert v1.3.0
	golang.org/x/crypto v0.36.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	cloud.google.com/go/compute/metadata v0.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/go-errors/errors v1.5.1 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/viant/tagly v0.2.0 // indirect
	github.com/viant/toolbox v0.36.0 // indirect
	github.com/viant/xreflect v0.7.2 // indirect
	github.com/viant/xunsafe v0.9.3-0.20240530173106-69808f27713b // indirect
	golang.org/x/mod v0.17.0 // indirect
	golang.org/x/oauth2 v0.19.0 // indirect
	golang.org/x/sys v0.31.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)

replace github.com/viant/structology => ../structology
