package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/code-sena/design-software-notification-service/api"
	otelplatform "github.com/code-sena/design-software-notification-service/internal/platform/otel"
)

// notificationExchange follows the same "<domain>-events" topic-exchange convention as
// scheduling-events/monitoring-events (ADR-001); audit-service's global fan-out
// subscription binds to it with routing key "#".
const notificationExchange = "notification-events"

// defaultTracer resolves against whatever global TracerProvider is configured at
// process startup. Not used by tests -- see the equivalent comment in
// adapter/in/amqp/consumer.go for why (otel's global delegate can only really be
// (re)assigned once per process).
var defaultTracer = otel.Tracer("notification-service/adapter/out/messaging")

// propagator is constructed directly (not via otel.GetTextMapPropagator()); see the
// equivalent comment in adapter/in/amqp/consumer.go.
var propagator = propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})

// OutboxRelay publishes staged notification.outbox rows to RabbitMQ and marks them
// published. It talks to Postgres and the broker directly (no application port): this
// is infrastructure plumbing, not business logic -- the use case already decided what
// to stage (see usecase.ConsumeDomainEvent), the relay's only job is "get it to the
// broker eventually".
type OutboxRelay struct {
	pool   *pgxpool.Pool
	ch     *amqp091.Channel
	tracer trace.Tracer
}

func NewOutboxRelay(pool *pgxpool.Pool, ch *amqp091.Channel) (*OutboxRelay, error) {
	if err := ch.ExchangeDeclare(notificationExchange, "topic", true, false, false, false, nil); err != nil {
		return nil, err
	}
	return &OutboxRelay{pool: pool, ch: ch, tracer: defaultTracer}, nil
}

type stagedEvent struct {
	id        string
	eventID   string
	eventType string
	payload   []byte
	createdAt time.Time
}

// PollAndPublish publishes up to limit unpublished rows (oldest first) and marks each
// published_at as it succeeds, all in one transaction with FOR UPDATE SKIP LOCKED (safe
// for multiple relay instances running concurrently). If a publish fails partway
// through, the transaction rolls back -- rows already published to the broker in this
// batch stay unpublished in the DB and will be retried (and republished) on the next
// poll. That is a deliberate at-least-once trade-off: consumers (e.g. audit-service)
// are expected to dedup by event_id, same as this service does for its own inbound
// events.
func (r *OutboxRelay) PollAndPublish(ctx context.Context, limit int) (int, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	rows, err := tx.Query(ctx, `
		SELECT id::text, event_id::text, event_type, payload, created_at
		FROM notification.outbox
		WHERE published_at IS NULL
		ORDER BY created_at
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return 0, err
	}
	var staged []stagedEvent
	for rows.Next() {
		var s stagedEvent
		if err := rows.Scan(&s.id, &s.eventID, &s.eventType, &s.payload, &s.createdAt); err != nil {
			rows.Close()
			return 0, err
		}
		staged = append(staged, s)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	published := 0
	for _, s := range staged {
		if err := r.publishOne(ctx, s); err != nil {
			return published, err
		}
		if _, err := tx.Exec(ctx, `UPDATE notification.outbox SET published_at = now() WHERE id = $1::uuid`, s.id); err != nil {
			return published, fmt.Errorf("mark outbox event %s published: %w", s.eventID, err)
		}
		published++
	}

	return published, tx.Commit(ctx)
}

// publishOne continues the original trace (extracted from the payload's trace_parent,
// staged by the use case that consumed the inbound event -- HU-NOTIF-007 §"cruce el
// salto async") when present, and injects its own span's context into the outgoing
// AMQP headers so a downstream consumer (audit-service) can continue it further.
func (r *OutboxRelay) publishOne(ctx context.Context, s stagedEvent) error {
	var payload map[string]interface{}
	if err := json.Unmarshal(s.payload, &payload); err != nil {
		return fmt.Errorf("unmarshal outbox payload for %s: %w", s.eventID, err)
	}

	publishCtx := ctx
	if tp, ok := payload["trace_parent"].(string); ok && tp != "" {
		publishCtx = propagator.Extract(ctx, propagation.MapCarrier{"traceparent": tp})
	}
	publishCtx, span := r.tracer.Start(publishCtx, "outbox.publish "+s.eventType,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "rabbitmq"),
			attribute.String("messaging.destination", notificationExchange),
			attribute.String("event.type", s.eventType),
			attribute.String("event.id", s.eventID),
		),
	)
	defer span.End()

	env := api.DomainEventEnvelope{
		EventID:       s.eventID,
		EventType:     s.eventType,
		Version:       "1.0",
		Timestamp:     s.createdAt,
		SourceService: "notification-service",
		Payload:       payload,
	}
	body, err := json.Marshal(env)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("marshal envelope for %s: %w", s.eventID, err)
	}

	headers := amqp091.Table{}
	propagator.Inject(publishCtx, otelplatform.AMQPHeaderCarrier(headers))

	if err := r.ch.PublishWithContext(publishCtx, notificationExchange, s.eventType, false, false, amqp091.Publishing{
		ContentType: "application/json",
		MessageId:   s.eventID,
		Headers:     headers,
		Body:        body,
	}); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("publish outbox event %s: %w", s.eventID, err)
	}
	return nil
}

// Run polls every interval until ctx is done, logging (not failing hard on) transient
// errors -- a broker/DB blip should not kill the worker process.
func (r *OutboxRelay) Run(ctx context.Context, interval time.Duration, limit int, onError func(error)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := r.PollAndPublish(ctx, limit); err != nil && onError != nil {
				onError(err)
			}
		}
	}
}
