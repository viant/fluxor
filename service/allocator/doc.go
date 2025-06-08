// Package allocator owns the execution queue and is the only service allowed
// to mutate `Process` instances according to the project guidelines.  It is
// responsible for scheduling tasks and reporting their status back to the
// executor/processor layer.
package allocator
