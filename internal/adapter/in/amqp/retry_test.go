package amqp

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/code-sena/design-software-notification-service/internal/application/port/in"
	"github.com/code-sena/design-software-notification-service/internal/domain/model"
	otelplatform "github.com/code-sena/design-software-notification-service/internal/platform/otel"
)

// fakeAcknowledger records Ack/Nack calls without a live broker.
type fakeAcknowledger struct {
	acked   bool
	nacked  bool
	requeue bool
}

func (f *fakeAcknowledger) Ack(_ uint64, _ bool) error { f.acked = true; return nil }
func (f *fakeAcknowledger) Nack(_ uint64, _ bool, requeue bool) error {
	f.nacked = true
	f.requeue = requeue
	return nil
}
func (f *fakeAcknowledger) Reject(_ uint64, _ bool) error { return nil }

// countingUseCase counts Handle calls and pops errors from a queue.
type countingUseCase struct {
	calls  int
	errors []error // each call pops from front; once empty, returns nil
}

func (c *countingUseCase) Handle(_ context.Context, _ in.ConsumeDomainEventCommand) error {
	c.calls++
	if len(c.errors) == 0 {
		return nil
	}
	err := c.errors[0]
	c.errors = c.errors[1:]
	return err
}

// noopRetryMetrics builds a Metrics with noop instruments (no real OTEL provider needed).
func noopRetryMetrics(t *testing.T) *otelplatform.Metrics {
	t.Helper()
	m, err := otelplatform.NewMetrics(noop.NewMeterProvider().Meter("test"))
	if err != nil {
		t.Fatalf("failed to build noop metrics: %v", err)
	}
	return m
}

// noSleep is a sleepFn that returns immediately — avoids real waits in retry tests.
func noSleep(_ time.Duration) {}

// retryConsumer builds a Consumer wired for retry tests: private tracer, noop metrics,
// instant sleep, and the supplied RetryConfig.
func retryConsumer(t *testing.T, uc in.ConsumeDomainEventUseCase, cfg RetryConfig) *Consumer {
	t.Helper()
	c, _ := newTestConsumer(uc) // sets c.uc + c.tracer from a private TracerProvider
	c.metrics = noopRetryMetrics(t)
	c.retryCfg = cfg
	c.sleepFn = noSleep
	return c
}

// fakeDelivery builds an amqp091.Delivery with a fakeAcknowledger and the given event body.
func fakeDelivery(t *testing.T, ack *fakeAcknowledger, eventID string) amqp091.Delivery {
	t.Helper()
	return amqp091.Delivery{
		Acknowledger: ack,
		Body:         alertEnvelopeBody(t, eventID),
	}
}

// HU-NOTIF-004 E1: transient error on first two attempts → retries and Acks on 3rd success.
func TestConsumer_Handle_RetriesTransientErrorAndAcksOnSuccess(t *testing.T) {
	transient := errors.New("smtp: connection refused")
	uc := &countingUseCase{errors: []error{transient, transient}} // fail twice, succeed 3rd
	cfg := RetryConfig{MaxAttempts: 3, InitialInterval: time.Millisecond}
	ack := &fakeAcknowledger{}

	retryConsumer(t, uc, cfg).handle(context.Background(), fakeDelivery(t, ack, "evt-retry-1"))

	if uc.calls != 3 {
		t.Fatalf("want 3 use-case calls (2 failures + 1 success), got %d", uc.calls)
	}
	if !ack.acked {
		t.Fatal("expected Ack after eventual success")
	}
	if ack.nacked {
		t.Fatal("must not Nack when delivery eventually succeeds")
	}
}

// HU-NOTIF-004 E2: all retries exhausted → Nack(requeue=false) routes to DLQ.
func TestConsumer_Handle_NacksToDeadLetterAfterMaxRetries(t *testing.T) {
	transient := errors.New("db: connection pool exhausted")
	uc := &countingUseCase{
		errors: []error{transient, transient, transient, transient}, // always fail
	}
	cfg := RetryConfig{MaxAttempts: 3, InitialInterval: time.Millisecond}
	ack := &fakeAcknowledger{}

	retryConsumer(t, uc, cfg).handle(context.Background(), fakeDelivery(t, ack, "evt-dlq-1"))

	if uc.calls != 3 {
		t.Fatalf("want exactly %d use-case calls (MaxAttempts), got %d", cfg.MaxAttempts, uc.calls)
	}
	if !ack.nacked {
		t.Fatal("expected Nack after max retries exhausted")
	}
	if ack.requeue {
		t.Fatal("Nack must have requeue=false so RabbitMQ routes to DLQ")
	}
}

// HU-NOTIF-004 E2: permanent error → Nack immediately without any retry attempt.
func TestConsumer_Handle_NacksImmediatelyOnPermanentError(t *testing.T) {
	uc := &countingUseCase{
		errors: []error{fmt.Errorf("bad event_type: %w", model.ErrPermanentFailure)},
	}
	cfg := RetryConfig{MaxAttempts: 5, InitialInterval: time.Millisecond}
	ack := &fakeAcknowledger{}

	retryConsumer(t, uc, cfg).handle(context.Background(), fakeDelivery(t, ack, "evt-perm-1"))

	if uc.calls != 1 {
		t.Fatalf("want exactly 1 use-case call for a permanent error, got %d", uc.calls)
	}
	if !ack.nacked {
		t.Fatal("expected Nack on permanent error")
	}
	if ack.requeue {
		t.Fatal("Nack must have requeue=false for permanent errors")
	}
}

// HU-NOTIF-004 E1 (metric): retry path must not panic (noop meter records silently).
func TestConsumer_Handle_RetryMetricDoesNotPanic(t *testing.T) {
	transient := errors.New("smtp: timeout")
	uc := &countingUseCase{errors: []error{transient, transient}}
	cfg := RetryConfig{MaxAttempts: 3, InitialInterval: time.Millisecond}
	ack := &fakeAcknowledger{}

	retryConsumer(t, uc, cfg).handle(context.Background(), fakeDelivery(t, ack, "evt-metric-1"))

	if !ack.acked {
		t.Fatal("expected Ack after eventual success on 3rd attempt")
	}
}
