// Package tracing integrates observability back-ends with the Fluxor engine
// to provide distributed tracing information.  All instrumentation is kept in
// a separate package so that applications which do not require tracing can
// exclude it from their build.
package tracing
