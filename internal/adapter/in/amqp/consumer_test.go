package amqp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/code-sena/design-software-notification-service/api"
	"github.com/code-sena/design-software-notification-service/internal/application/port/in"
	otelplatform "github.com/code-sena/design-software-notification-service/internal/platform/otel"
)

type fakeUseCase struct {
	calledCmd in.ConsumeDomainEventCommand
	called    bool
	err       error
}

func (f *fakeUseCase) Handle(_ context.Context, cmd in.ConsumeDomainEventCommand) error {
	f.called = true
	f.calledCmd = cmd
	return f.err
}

// newTestConsumer builds a Consumer with its own private TracerProvider/exporter
// (never touching the process-global otel state, which can only really be swapped
// once per binary -- see the comment on defaultTracer).
func newTestConsumer(uc in.ConsumeDomainEventUseCase) (*Consumer, *tracetest.InMemoryExporter) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	return &Consumer{uc: uc, tracer: tp.Tracer("test")}, exporter
}

func alertEnvelopeBody(t *testing.T, eventID string) []byte {
	t.Helper()
	env := api.DomainEventEnvelope{
		EventID:       eventID,
		EventType:     routingKeyAlertTriggered,
		Version:       "1.0",
		Timestamp:     time.Now(),
		SourceService: "monitoring-service",
		Payload: map[string]interface{}{
			"affected_entity_type": "Learner",
			"affected_entity_id":   "10101010-1010-1010-1010-101010101010",
		},
	}
	body, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("failed to marshal test envelope: %v", err)
	}
	return body
}

// E1: a valid delivery makes the consumer emit exactly one span, and delegates to the
// use case.
func TestConsumer_Handle_EmitsSpanAndCallsUseCase(t *testing.T) {
	uc := &fakeUseCase{}
	c, exporter := newTestConsumer(uc)

	c.handle(context.Background(), amqp091.Delivery{Body: alertEnvelopeBody(t, "evt-1")})

	if !uc.called {
		t.Fatal("use case was not called")
	}
	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("want 1 span, got %d", len(spans))
	}
	if spans[0].Name != "amqp.consume "+routingKeyAlertTriggered {
		t.Fatalf("unexpected span name: %s", spans[0].Name)
	}
}

// E2: when the AMQP delivery carries an upstream traceparent header, the consumer's
// span extracts and continues that trace instead of starting a disconnected one
// ("extraelo al consumir").
func TestConsumer_Handle_ExtractsUpstreamTraceparent(t *testing.T) {
	uc := &fakeUseCase{}
	c, exporter := newTestConsumer(uc)

	// Build a real upstream span/traceparent the same way OutboxRelay's publish would.
	upstreamExporter := tracetest.NewInMemoryExporter()
	upstreamTP := sdktrace.NewTracerProvider(sdktrace.WithSyncer(upstreamExporter))
	upstreamCtx, upstreamSpan := upstreamTP.Tracer("upstream-publisher").Start(context.Background(), "publish")
	headers := amqp091.Table{}
	propagation.TraceContext{}.Inject(upstreamCtx, otelplatform.AMQPHeaderCarrier(headers))
	upstreamSpan.End()
	upstreamTraceID := upstreamExporter.GetSpans()[0].SpanContext.TraceID()

	c.handle(context.Background(), amqp091.Delivery{
		Headers: headers,
		Body:    alertEnvelopeBody(t, "evt-2"),
	})

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("want 1 span, got %d", len(spans))
	}
	if spans[0].SpanContext.TraceID() != upstreamTraceID {
		t.Fatalf("consume span trace_id=%s does not match upstream trace_id=%s -- traceparent was not extracted",
			spans[0].SpanContext.TraceID(), upstreamTraceID)
	}
}

// E3: the consumer hands the use case a TraceParent string for the CURRENT (consume)
// span, so a later, decoupled OutboxRelay publish can continue the same trace
// ("para que la traza cruce el salto async").
func TestConsumer_Handle_PropagatesTraceParentToUseCase(t *testing.T) {
	uc := &fakeUseCase{}
	c, exporter := newTestConsumer(uc)

	c.handle(context.Background(), amqp091.Delivery{Body: alertEnvelopeBody(t, "evt-3")})

	if uc.calledCmd.TraceParent == "" {
		t.Fatal("ConsumeDomainEventCommand.TraceParent was not set")
	}
	consumeTraceID := exporter.GetSpans()[0].SpanContext.TraceID().String()
	if !strings.Contains(uc.calledCmd.TraceParent, consumeTraceID) {
		t.Fatalf("TraceParent %q does not carry the consume span's trace_id %q", uc.calledCmd.TraceParent, consumeTraceID)
	}
}

// E4: an invalid envelope is rejected (Nack, no requeue) without ever calling the use
// case, and without panicking on a Delivery that has no real Acknowledger (unit test,
// no broker).
func TestConsumer_Handle_InvalidEnvelope_RejectsWithoutCallingUseCase(t *testing.T) {
	uc := &fakeUseCase{}
	c, _ := newTestConsumer(uc)

	c.handle(context.Background(), amqp091.Delivery{Body: []byte("not json")})

	if uc.called {
		t.Fatal("use case should not be called for an undecodable envelope")
	}
}
