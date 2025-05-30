package fluxor

import (
	"github.com/viant/afs/storage"
	"github.com/viant/fluxor/model/types"
	execution2 "github.com/viant/fluxor/runtime/execution"
	"github.com/viant/fluxor/service/approval"
	"github.com/viant/fluxor/service/dao"
	"github.com/viant/fluxor/service/event"
	"github.com/viant/fluxor/service/messaging"
	"github.com/viant/fluxor/service/meta"
	"github.com/viant/fluxor/tracing"
	"github.com/viant/x"

	"github.com/viant/fluxor/service/executor"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Service represents fluxor service
type Option func(s *Service)

// WithApprovalService sets the approvalService service
func WithApprovalService(svc approval.Service) Option {
	return func(s *Service) { s.approvalService = svc }
}

// WithExtensionTypes sets the extension types
func WithExtensionTypes(types ...*x.Type) Option {
	return func(s *Service) {
		s.extensionTypes = types
	}
}

func WithEventService(service *event.Service) Option {
	return func(s *Service) {
		s.eventService = service
	}
}

// WithMetaService sets the meta service
func WithMetaService(service *meta.Service) Option {
	return func(s *Service) {
		s.metaService = service
	}
}

// WithExtensionServices sets the extension services
func WithExtensionServices(services ...types.Service) Option {
	return func(s *Service) {
		s.extensionServices = services
	}
}

// WithQueue sets the message queue
func WithQueue(queue messaging.Queue[execution2.Execution]) Option {
	return func(s *Service) {
		s.queue = queue
	}
}

// WithRootTaskNodeName sets the root task node name
func WithRootTaskNodeName(name string) Option {
	return func(s *Service) {
		s.rootTaskNodeName = name
	}
}

// WithProcessDAO sets the processor DAO
func WithProcessDAO(dao dao.Service[string, execution2.Process]) Option {
	return func(s *Service) {
		s.runtime.processorDAO = dao
	}
}

// WithTaskExecutionDAO sets the task execution DAO
func WithTaskExecutionDAO(dao dao.Service[string, execution2.Execution]) Option {
	return func(s *Service) {
		s.runtime.taskExecutionDao = dao
	}
}

// WithProcessorWorkers sets the processor workers
func WithProcessorWorkers(count int) Option {
	return func(s *Service) {
		s.processorWorkers = count
	}
}

// WithExecutorOptions lets the caller supply additional options passed to
// executor.NewService (e.g. disabling the default StdoutListener).
func WithExecutorOptions(opts ...executor.Option) Option {
	return func(s *Service) {
		s.executorOptions = append(s.executorOptions, opts...)
	}
}

// WithMetaBaseURL sets the meta base URL
func WithMetaBaseURL(url string) Option {
	return func(s *Service) {
		s.metaBaseURL = url
	}
}

// WithMetaFsOptions with meta file system options
func WithMetaFsOptions(options ...storage.Option) Option {
	return func(s *Service) {
		s.metaFsOptions = options
	}
}

// WithTracing configures OpenTelemetry tracing for the service. If outputFile is empty the
// stdout exporter is used; otherwise traces are written to the supplied file path. The function is
// safe to call multiple times – the first successful initialisation wins.
func WithTracing(serviceName, serviceVersion, outputFile string) Option {
	return func(s *Service) {
		_ = tracing.Init(serviceName, serviceVersion, outputFile)
	}
}

// WithTracingExporter configures OpenTelemetry tracing using a custom SpanExporter. This enables
// integrations with exporters other than the built-in stdout exporter, for example OTLP, Jaeger or
// Zipkin. The function is safe to call multiple times – the first successful initialisation wins.
func WithTracingExporter(serviceName, serviceVersion string, exporter sdktrace.SpanExporter) Option {
	return func(s *Service) {
		_ = tracing.InitWithExporter(serviceName, serviceVersion, exporter)
	}
}
