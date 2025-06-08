// Package idgen wraps the UUID generator so that it can be stubbed in tests.
// It lives under `internal` because callers should not rely on its exact
// behaviour or API – they should treat identifiers as opaque strings.
package idgen
