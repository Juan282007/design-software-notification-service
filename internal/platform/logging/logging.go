// Package logging provides structured JSON logging with trace/span id correlation
// (HU-NOTIF-007). Bootstrap-only, like internal/platform/otel: adapters use FromContext
// to get a logger already tagged with the active trace, domain/application don't import
// this package.
package logging

import (
	"context"
	"log/slog"
	"os"

	"go.opentelemetry.io/otel/trace"
)

// New returns a slog.Logger writing structured JSON to stdout. Loki (ADR-008) ingests
// container stdout in this project's stack, so no separate log exporter is wired here.
func New() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, nil))
}

// FromContext returns logger with trace_id/span_id attributes from ctx's active span,
// if any -- this is the "trace_id embebido" requirement from HU-NOTIF-007/ADR-008.
func FromContext(ctx context.Context, logger *slog.Logger) *slog.Logger {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return logger
	}
	return logger.With(
		slog.String("trace_id", sc.TraceID().String()),
		slog.String("span_id", sc.SpanID().String()),
	)
}
