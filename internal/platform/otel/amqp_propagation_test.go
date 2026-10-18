package otel

import (
	"context"
	"testing"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// TestAMQPHeaderCarrier_RoundTrip proves the mechanism HU-NOTIF-007 relies on to cross
// the async hop: a traceparent injected into AMQP headers on the "publish" side can be
// extracted on the "consume" side and continues the same trace (same TraceID).
func TestAMQPHeaderCarrier_RoundTrip(t *testing.T) {
	prop := propagation.TraceContext{}
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	tracer := tp.Tracer("test")

	// "Publish" side: start a span, inject it into AMQP headers.
	publishCtx, publishSpan := tracer.Start(context.Background(), "publish")
	headers := amqp091.Table{}
	prop.Inject(publishCtx, AMQPHeaderCarrier(headers))
	publishSpan.End()

	if _, ok := headers["traceparent"]; !ok {
		t.Fatal("Inject did not set a traceparent header")
	}

	// "Consume" side: extract from the same headers, start a child span.
	consumeCtx := prop.Extract(context.Background(), AMQPHeaderCarrier(headers))
	_, consumeSpan := tracer.Start(consumeCtx, "consume")
	consumeSpan.End()

	spans := exporter.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("want 2 spans (publish+consume), got %d", len(spans))
	}
	publishTraceID := spans[0].SpanContext.TraceID()
	consumeTraceID := spans[1].SpanContext.TraceID()
	if publishTraceID != consumeTraceID {
		t.Fatalf("trace did not cross the AMQP hop: publish trace_id=%s, consume trace_id=%s", publishTraceID, consumeTraceID)
	}
	if spans[1].Parent.SpanID() != spans[0].SpanContext.SpanID() {
		t.Fatal("consume span is not a child of the publish span")
	}
}

// TestAMQPHeaderCarrier_GetSetKeys is a plain interface-contract check.
func TestAMQPHeaderCarrier_GetSetKeys(t *testing.T) {
	c := AMQPHeaderCarrier(amqp091.Table{})
	c.Set("traceparent", "00-aaaa-bbbb-01")
	if got := c.Get("traceparent"); got != "00-aaaa-bbbb-01" {
		t.Fatalf("Get after Set = %q", got)
	}
	if got := c.Get("missing"); got != "" {
		t.Fatalf("Get for missing key = %q, want empty", got)
	}
	c.Set("tracestate", "vendor=value")
	keys := c.Keys()
	if len(keys) != 2 {
		t.Fatalf("want 2 keys, got %d: %v", len(keys), keys)
	}
}
