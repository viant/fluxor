package tracing

// Package tracing provides a thin wrapper around OpenTelemetry tracing so that the rest of the
// code-base can continue to use the previously defined helper functions (StartSpan, EndSpan, etc.)
// without being concerned with the underlying implementation. All new functionality is delegated
// to OpenTelemetry and nothing is re-implemented unless it is not available in the upstream
// library.

import (
	"context"
	"io"
	"os"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Trace is kept only so that the public API of the package remains the same. It no longer contains
// any fields because OpenTelemetry manages traces internally. The value can be used as a marker by
// callers that still expect it.
type Trace struct{}

// NewTrace is retained for backward compatibility. It returns an empty *Trace and makes sure that
// the global tracer provider is initialised. The function is idempotent.
func NewTrace(serviceName, serviceVersion string) *Trace {
	// Initialise tracing with the default stdout exporter writing to os.Stdout. The error is
	// intentionally ignored – if initialisation fails the returned *Trace can still be used as a
	// dummy value causing spans to be no-op.
	_ = Init(serviceName, serviceVersion, "")
	return &Trace{}
}

// Init configures OpenTelemetry with the stdout exporter. If outputFile is an empty string the
// exporter uses os.Stdout; otherwise the traces are written to the specified file. The function is
// safe to call multiple times – the first successful initialisation wins.
// Init configures OpenTelemetry with the stdout exporter backed by either os.Stdout or the
// specified file. If outputFile is an empty string traces are written to os.Stdout. The function
// is safe to call multiple times – the first successful initialisation wins.
func Init(serviceName, serviceVersion, outputFile string) error {
	var w io.Writer = os.Stdout
	if outputFile != "" {
		f, err := os.Create(outputFile)
		if err != nil {
			return err
		}
		w = f
	}

	exporter, err := stdouttrace.New(stdouttrace.WithWriter(w))
	if err != nil {
		return err
	}
	return installProvider(serviceName, serviceVersion, exporter)
}

// InitWithExporter configures OpenTelemetry using the supplied SpanExporter. This allows callers
// to integrate with any exporter supported by the OpenTelemetry SDK (e.g. OTLP, Jaeger, Zipkin).
// The function is safe to call multiple times – the first successful initialisation wins.
func InitWithExporter(serviceName, serviceVersion string, exporter sdktrace.SpanExporter) error {
	return installProvider(serviceName, serviceVersion, exporter)
}

var (
	providerOnce sync.Once
	providerErr  error
)

// installProvider registers the supplied exporter as the global trace provider. The operation is
// executed only once; subsequent invocations are no-ops and return the error (if any) from the
// first attempt.
func installProvider(serviceName, serviceVersion string, exporter sdktrace.SpanExporter) error {
	if exporter == nil {
		return nil // Treat nil exporter as no-op but return nil to keep behaviour consistent with previous implementation
	}

	providerOnce.Do(func() {
		res, err := resource.New(context.Background(),
			resource.WithAttributes(
				attribute.String("service.name", serviceName),
				attribute.String("service.version", serviceVersion),
			),
		)
		if err != nil {
			providerErr = err
			return
		}

		tp := sdktrace.NewTracerProvider(
			sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(exporter)),
			sdktrace.WithResource(res),
		)

		otel.SetTracerProvider(tp)
	})

	return providerErr
}

// Span wraps go.opentelemetry.io/otel/trace.Span so that the callers do not need to import the
// upstream package directly.
type Span struct {
	span trace.Span
}

// WithAttributes attaches all provided attributes to the span.
func (s *Span) WithAttributes(attrs map[string]string) *Span {
	if s == nil {
		return s
	}
	if len(attrs) == 0 {
		return s
	}
	otelAttrs := make([]attribute.KeyValue, 0, len(attrs))
	for k, v := range attrs {
		otelAttrs = append(otelAttrs, attribute.String(k, v))
	}
	s.span.SetAttributes(otelAttrs...)
	return s
}

// SetStatus records an error status on the span. If err is nil an OK status is recorded instead.
func (s *Span) SetStatus(err error) {
	if s == nil {
		return
	}
	if err != nil {
		s.span.RecordError(err)
		s.span.SetStatus(codes.Error, err.Error())
	} else {
		s.span.SetStatus(codes.Ok, "")
	}
}

// SetStatusFromHTTPCode sets the span’s status based on an HTTP response code.
func (s *Span) SetStatusFromHTTPCode(code int) {
	if s == nil {
		return
	}

	switch {
	case code >= 100 && code < 400:
		s.span.SetStatus(codes.Ok, "")
	case code >= 400 && code < 500:
		s.span.SetStatus(codes.Error, "client error")
	case code >= 500:
		s.span.SetStatus(codes.Error, "server error")
	default:
		s.span.SetStatus(codes.Unset, "")
	}
}

// OnDone is kept for backward compatibility; in the OpenTelemetry world span.End() is used instead
// so this function simply calls End.
func (s *Span) OnDone() {
	if s == nil {
		return
	}
	s.span.End()
}

// StartSpan starts a new child span using OpenTelemetry. The string "kind" is mapped onto the
// appropriate trace.SpanKind value; when the mapping cannot be determined SpanKindInternal is used
// as a sensible default.
func StartSpan(ctx context.Context, name, kind string) (context.Context, *Span) {
	tracer := otel.Tracer("github.com/viant/fluxor")

	var spanKind trace.SpanKind
	switch kind {
	case "SERVER":
		spanKind = trace.SpanKindServer
	case "CLIENT":
		spanKind = trace.SpanKindClient
	case "PRODUCER":
		spanKind = trace.SpanKindProducer
	case "CONSUMER":
		spanKind = trace.SpanKindConsumer
	default:
		spanKind = trace.SpanKindInternal
	}

	// Capture any parent span present in the incoming context *before* starting the new span so we
	// can explicitly annotate lineage later on.
	parentSpan := trace.SpanFromContext(ctx)

	ctx, span := tracer.Start(ctx, name, trace.WithSpanKind(spanKind))

	// If a parent span was found, annotate the newly-created span with its identifiers. While the
	// OpenTelemetry SDK already maintains the relationship internally, recording those IDs as
	// attributes helps when consumers (or tests) inspect a span in isolation.
	if parentSpan != nil {
		if sc := parentSpan.SpanContext(); sc.IsValid() {
			span.SetAttributes(
				attribute.String("parent.trace_id", sc.TraceID().String()),
				attribute.String("parent.span_id", sc.SpanID().String()),
			)
		}
	}

	return ctx, &Span{span: span}
}

// EndSpan finalises the span and records status depending on the provided error.
func EndSpan(sp *Span, err error) {
	if sp == nil {
		return
	}
	sp.SetStatus(err)
	sp.span.End()
}

// WithTrace is retained purely for compatibility and presently returns the received context
// unchanged because OpenTelemetry does not require additional trace data to be stored inside the
// context by the callers.
func WithTrace(ctx context.Context, _ *Trace) context.Context {
	return ctx
}

// TraceFromContext returns (nil, false) because Trace is no longer stored in the context; the
// function is provided so that callers continue to compile.
func TraceFromContext(context.Context) (*Trace, bool) {
	return nil, false
}

// WithSpan attaches the OpenTelemetry span wrapped by *Span to the context.
func WithSpan(ctx context.Context, sp *Span) context.Context {
	if sp == nil {
		return ctx
	}
	return trace.ContextWithSpan(ctx, sp.span)
}

// SpanFromContext retrieves the *Span wrapper from the supplied context when present.
func SpanFromContext(ctx context.Context) (*Span, bool) {
	sp := trace.SpanFromContext(ctx)
	if sp == nil {
		return nil, false
	}
	return &Span{span: sp}, true
}
