package tracing

import (
	"context"
	"os"
	"testing"
)

func TestTracingFile(t *testing.T) {
	fname := "testdata/span_test.txt"
	_ = os.Remove(fname)

	if err := Init("fluxor", "0.0.1", fname); err != nil {
		t.Fatalf("init failed: %v", err)
	}

	ctx, span := StartSpan(context.Background(), "test", "INTERNAL")
	span.WithAttributes(map[string]string{"k": "v"})
	EndSpan(span, nil)
	_ = ctx

	data, err := os.ReadFile(fname)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if len(data) == 0 {
		t.Fatalf("no data written to trace file")
	}
}
