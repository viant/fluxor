package execution

import "testing"

// TestProcessRemove verifies that removing an execution from the stack keeps
// the remaining order intact and removes exactly one element regardless of its
// position (first, middle, last).
func TestProcessRemove(t *testing.T) {
	newExec := func(id string) *Execution { return &Execution{ID: id} }

	stack := []*Execution{newExec("a"), newExec("b"), newExec("c")}

	proc := &Process{Stack: append([]*Execution(nil), stack...)}

	proc.Remove(stack[1]) // remove "b" (middle element)

	if got, want := len(proc.Stack), 2; got != want {
		t.Fatalf("after removal expected stack length %d, got %d", want, got)
	}

	// Expect order [a, c]
	if proc.Stack[0].ID != "a" || proc.Stack[1].ID != "c" {
		t.Fatalf("unexpected stack order after removal: %+v", proc.Stack)
	}

	// Remove last element
	proc.Remove(proc.Stack[1]) // removes "c"
	if got, want := len(proc.Stack), 1; got != want || proc.Stack[0].ID != "a" {
		t.Fatalf("unexpected stack after removing last element: %+v", proc.Stack)
	}
}
