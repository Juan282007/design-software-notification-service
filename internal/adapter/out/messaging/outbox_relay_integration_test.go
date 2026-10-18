//go:build integration

package messaging

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/google/uuid"

	"github.com/code-sena/design-software-notification-service/internal/adapter/out/persistence"
)

// TestOutboxRelay_PublishesStagedEvents_Integration seeds a notification.outbox row
// directly, runs PollAndPublish, and checks both that published_at got set and that the
// event was actually received on the notification-events exchange.
//
// KNOWN BLOCKED as of this PR: notification.outbox does not exist locally yet (see the
// design-software-notification-db PR referenced in the HU-NOTIF-002 report) -- the seed
// INSERT below will fail with "relation notification.outbox does not exist" until that
// migration is applied. Expected to go green with no code changes once it is.
func TestOutboxRelay_PublishesStagedEvents_Integration(t *testing.T) {
	dbDSN := os.Getenv("NOTIFICATION_DB_DSN")
	amqpURL := os.Getenv("NOTIFICATION_AMQP_URL")
	if dbDSN == "" || amqpURL == "" {
		t.Skip("NOTIFICATION_DB_DSN / NOTIFICATION_AMQP_URL not set; skipping integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool, err := persistence.NewPool(ctx, dbDSN)
	if err != nil {
		t.Fatalf("failed to open notification_db pool: %v", err)
	}
	defer pool.Close()

	conn, err := amqp091.Dial(amqpURL)
	if err != nil {
		t.Fatalf("failed to dial amqp: %v", err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open amqp channel: %v", err)
	}
	defer ch.Close()

	relay, err := NewOutboxRelay(pool, ch)
	if err != nil {
		t.Fatalf("failed to start relay: %v", err)
	}

	// A second channel/queue standing in for a consumer of the fan-out exchange
	// (audit-service, in production), bound with "#" per ADR-001.
	consumerCh, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open consumer channel: %v", err)
	}
	defer consumerCh.Close()
	if err := consumerCh.ExchangeDeclare(notificationExchange, "topic", true, false, false, false, nil); err != nil {
		t.Fatalf("failed to declare %s: %v", notificationExchange, err)
	}
	q, err := consumerCh.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatalf("failed to declare test queue: %v", err)
	}
	if err := consumerCh.QueueBind(q.Name, "#", notificationExchange, false, nil); err != nil {
		t.Fatalf("failed to bind test queue: %v", err)
	}
	deliveries, err := consumerCh.Consume(q.Name, "", true, true, false, false, nil)
	if err != nil {
		t.Fatalf("failed to consume test queue: %v", err)
	}

	eventID := uuid.NewString()
	payload, _ := json.Marshal(map[string]string{
		"notification_id": uuid.NewString(),
		"recipient_id":     "integration-test-recipient",
		"channel":          "EMAIL",
	})
	var outboxID string
	err = pool.QueryRow(ctx, `
		INSERT INTO notification.outbox (event_id, event_type, payload)
		VALUES ($1::uuid, 'notification.notification.sent', $2::jsonb)
		RETURNING id::text`,
		eventID, payload,
	).Scan(&outboxID)
	if err != nil {
		t.Fatalf("failed to seed outbox row (see doc comment -- likely the pending migration): %v", err)
	}

	published, err := relay.PollAndPublish(ctx, 10)
	if err != nil {
		t.Fatalf("PollAndPublish failed: %v", err)
	}
	if published < 1 {
		t.Fatalf("want at least 1 published event, got %d", published)
	}

	select {
	case d := <-deliveries:
		if d.Type == "" && string(d.Body) == "" {
			t.Fatal("received an empty delivery")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the relay to publish to notification-events")
	}

	var publishedAt *time.Time
	if err := pool.QueryRow(ctx, `SELECT published_at FROM notification.outbox WHERE id = $1::uuid`, outboxID).Scan(&publishedAt); err != nil {
		t.Fatalf("failed to read back outbox row: %v", err)
	}
	if publishedAt == nil {
		t.Fatal("published_at was not set")
	}

	_, _ = pool.Exec(ctx, `DELETE FROM notification.outbox WHERE id = $1::uuid`, outboxID)
}
