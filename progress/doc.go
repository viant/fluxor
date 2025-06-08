// Package progress defines primitives for reporting and aggregating the
// progress of long-running tasks executed by the Fluxor runtime.  It abstracts
// away the underlying communication mechanism so that callers can consume
// progress updates in a uniform way regardless of whether they are delivered
// via in-memory channels, message queues or external observers.
package progress
