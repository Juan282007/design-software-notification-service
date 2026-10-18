package amqp

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	backoff "github.com/cenkalti/backoff/v5"
	amqp091 "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/code-sena/design-software-notification-service/api"
	"github.com/code-sena/design-software-notification-service/internal/application/port/in"
	"github.com/code-sena/design-software-notification-service/internal/domain/model"
	otelplatform "github.com/code-sena/design-software-notification-service/internal/platform/otel"
)

// Topic exchanges this consumer binds to, one per upstream domain (ADR-001 convention:
// "<domain>-events"). Declared here (not just assumed) so the consumer works standalone
// even before scheduling-service/monitoring-service exist as code.
const (
	exchangeScheduling = "scheduling-events"
	exchangeMonitoring = "monitoring-events"

	routingKeySchedulePublished = "scheduling.schedule.published"
	routingKeyAlertTriggered    = "monitoring.alert.triggered"

	queueName = "notification-service.events"

	// DLQ topology (HU-NOTIF-004 E2): dead-letter exchange + queue that catch messages
	// after all retry attempts are exhausted. Declared by NewConsumer on startup.
	dlxExchange = "notification-dlx"
	dlqName     = "notification-service.events.dlq"
)

// defaultTracer resolves against whatever global TracerProvider is configured at
// process startup (internal/platform/otel.Setup). NOT used by tests: otel's global
// registry only lets the real delegate be assigned ONCE per process
// (sync.Once in go.opentelemetry.io/otel/internal/global), so swapping
// otel.SetTracerProvider between tests would silently stop working after the first
// test that calls it. Tests inject their own tracer directly (Consumer.tracer, an
// unexported field this package's tests can set via struct literal).
var defaultTracer = otel.Tracer("notification-service/adapter/in/amqp")

// propagator is constructed directly (not via otel.GetTextMapPropagator()) so this
// package's own W3C trace-context injection/extraction never depends on global otel
// state having been configured -- same rationale as defaultTracer above, and it
// matches what internal/platform/otel.Setup configures globally anyway.
var propagator = propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})

// RetryConfig controls the exponential-backoff retry behaviour for transient use-case
// errors (HU-NOTIF-004 E1). All fields are read from env by the composition root.
type RetryConfig struct {
	// MaxAttempts is the total number of delivery attempts (1 = no retry).
	// Set via NOTIFICATION_RETRY_MAX_ATTEMPTS (default 3).
	MaxAttempts int
	// InitialInterval is the first backoff pause duration.
	// Set via NOTIFICATION_RETRY_INITIAL_INTERVAL_MS (default 500 ms).
	InitialInterval time.Duration
	// MaxInterval caps exponential growth.
	// Set via NOTIFICATION_RETRY_MAX_INTERVAL_MS (default 30 s).
	MaxInterval time.Duration
}

// DefaultRetryConfig is the production default (3 attempts, 500 ms → up to 30 s).
var DefaultRetryConfig = RetryConfig{
	MaxAttempts:     3,
	InitialInterval: 500 * time.Millisecond,
	MaxInterval:     30 * time.Second,
}

// Consumer consumes scheduling.schedule.published and monitoring.alert.triggered from
// RabbitMQ (HU-NOTIF-002) and delegates each decoded envelope to the use case.
type Consumer struct {
	conn   *amqp091.Connection
	ch     *amqp091.Channel
	uc     in.ConsumeDomainEventUseCase
	tracer trace.Tracer
	// metrics holds OTel instruments; nil is safe (metrics skipped, used in tests).
	metrics  *otelplatform.Metrics
	retryCfg RetryConfig
	// sleepFn is time.Sleep in production; injected as a no-op in tests to skip delays.
	sleepFn func(time.Duration)
}

// NewConsumer dials amqpURL, declares the full topology (exchanges, DLQ, queue with
// dead-letter args, bindings) and returns a Consumer ready for Run.
// All declarations are idempotent — safe even if the broker already has matching entities.
func NewConsumer(amqpURL string, uc in.ConsumeDomainEventUseCase, metrics *otelplatform.Metrics, cfg RetryConfig) (*Consumer, error) {
	conn, err := amqp091.Dial(amqpURL)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	// Upstream event exchanges.
	for _, ex := range []string{exchangeScheduling, exchangeMonitoring} {
		if err := ch.ExchangeDeclare(ex, "topic", true, false, false, false, nil); err != nil {
			_ = ch.Close()
			_ = conn.Close()
			return nil, err
		}
	}

	// HU-NOTIF-004 E2: dead-letter topology.
	// 1. DLX — fanout so any future DLQ binding just works.
	if err := ch.ExchangeDeclare(dlxExchange, "fanout", true, false, false, false, nil); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, err
	}
	// 2. Dead-letter queue.
	if _, err := ch.QueueDeclare(dlqName, true, false, false, false, nil); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, err
	}
	// 3. Bind DLQ to DLX (routing key "" works with fanout).
	if err := ch.QueueBind(dlqName, "", dlxExchange, false, nil); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, err
	}

	// 4. Main queue WITH x-dead-letter-exchange so exhausted messages route to the DLQ.
	queueArgs := amqp091.Table{"x-dead-letter-exchange": dlxExchange}
	if _, err := ch.QueueDeclare(queueName, true, false, false, false, queueArgs); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, err
	}
	if err := ch.QueueBind(queueName, routingKeySchedulePublished, exchangeScheduling, false, nil); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, err
	}
	if err := ch.QueueBind(queueName, routingKeyAlertTriggered, exchangeMonitoring, false, nil); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, err
	}

	return &Consumer{
		conn:     conn,
		ch:       ch,
		uc:       uc,
		tracer:   defaultTracer,
		metrics:  metrics,
		retryCfg: cfg,
		sleepFn:  time.Sleep,
	}, nil
}

// Run consumes until ctx is done.
func (c *Consumer) Run(ctx context.Context) error {
	deliveries, err := c.ch.ConsumeWithContext(ctx, queueName, "notification-service", false, false, false, false, nil)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				return nil
			}
			c.handle(ctx, d)
		}
	}
}

func (c *Consumer) handle(ctx context.Context, d amqp091.Delivery) {
	var env api.DomainEventEnvelope
	if err := json.Unmarshal(d.Body, &env); err != nil {
		log.Printf("notification-worker: rejecting invalid envelope: %v", err)
		_ = d.Nack(false, false) // permanent: unparseable — send to DLQ
		return
	}

	propCtx := propagator.Extract(ctx, otelplatform.AMQPHeaderCarrier(d.Headers))
	spanCtx, span := c.tracer.Start(propCtx, "amqp.consume "+env.EventType,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "rabbitmq"),
			attribute.String("messaging.destination", queueName),
			attribute.String("event.type", env.EventType),
			attribute.String("event.id", env.EventID),
		),
	)
	defer span.End()

	carrier := propagation.MapCarrier{}
	propagator.Inject(spanCtx, carrier)

	cmd := in.ConsumeDomainEventCommand{
		EventID:       env.EventID,
		EventType:     env.EventType,
		SourceService: env.SourceService,
		Payload:       env.Payload,
		TraceParent:   carrier.Get("traceparent"),
	}

	// HU-NOTIF-004 E1/E2: exponential backoff retry loop.
	maxAttempts := c.retryCfg.MaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	b := backoff.NewExponentialBackOff()
	b.InitialInterval = c.retryCfg.InitialInterval
	b.MaxInterval = c.retryCfg.MaxInterval
	b.Reset()

	attempt := 0
	var lastErr error
	for {
		attempt++
		lastErr = c.uc.Handle(spanCtx, cmd)
		if lastErr == nil {
			_ = d.Ack(false)
			return
		}

		// Permanent failure: Nack immediately without retry.
		if errors.Is(lastErr, model.ErrPermanentFailure) {
			log.Printf("notification-worker: permanent failure for event %s: %v", env.EventID, lastErr)
			span.RecordError(lastErr)
			span.SetStatus(codes.Error, "permanent: "+lastErr.Error())
			_ = d.Nack(false, false)
			return
		}

		// Transient failure: record retry metric.
		if c.metrics != nil {
			c.metrics.AMQPRetriesTotal.Add(spanCtx, 1,
				metric.WithAttributes(
					attribute.String("event.id", env.EventID),
					attribute.Int("attempt", attempt),
				),
			)
		}

		if attempt >= maxAttempts {
			log.Printf("notification-worker: max retries (%d) exhausted for event %s: %v — routing to DLQ",
				maxAttempts, env.EventID, lastErr)
			span.RecordError(lastErr)
			span.SetStatus(codes.Error, "max retries exhausted: "+lastErr.Error())
			_ = d.Nack(false, false) // RabbitMQ routes to notification-dlx → DLQ
			return
		}

		wait := b.NextBackOff()
		log.Printf("notification-worker: attempt %d/%d for event %s failed (%v), retrying in %v",
			attempt, maxAttempts, env.EventID, lastErr, wait)

		select {
		case <-ctx.Done():
			_ = d.Nack(false, true) // requeue: context cancelled, not a message error
			return
		default:
			c.sleepFn(wait)
		}
	}
}

var errAMQPConnectionClosed = errors.New("amqp connection is closed")

// Ping reports whether the AMQP connection is usable (used by GET /ready).
func (c *Consumer) Ping(_ context.Context) error {
	if c.conn == nil || c.conn.IsClosed() {
		return errAMQPConnectionClosed
	}
	return nil
}

func (c *Consumer) Close() error {
	chErr := c.ch.Close()
	connErr := c.conn.Close()
	if chErr != nil {
		return chErr
	}
	return connErr
}
