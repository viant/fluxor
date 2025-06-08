// Package processor hosts the workers that execute individual task
// executions.  Every worker consumes items from the queue owned by the
// allocator and updates the execution state so that the allocator can decide
// what to schedule next.
package processor
