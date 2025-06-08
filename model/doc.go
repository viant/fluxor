// Package model contains the in-memory representation of workflow
// definitions, runtime state and supporting types used by the Fluxor engine.
//
// A workflow is typically loaded from a YAML or JSON document into the
// structures defined in the `graph`, `state` and `types` sub-packages.  The
// root model package simply aggregates those building blocks so that they can
// be referenced from other parts of the code base with a single import.
package model
