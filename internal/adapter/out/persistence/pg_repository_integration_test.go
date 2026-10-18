//go:build integration

package persistence

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/code-sena/design-software-notification-service/internal/domain/model"
)

func TestPgNotificationRepository_Save_Integration(t *testing.T) {
	dsn := os.Getenv("NOTIFICATION_DB_DSN")
	if dsn == "" {
		t.Skip("NOTIFICATION_DB_DSN not set; skipping integration test (no default credentials in code)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := NewPool(ctx, dsn)
	if err != nil {
		t.Fatalf("failed to open pool against notification_db: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("failed to ping notification_db: %v", err)
	}

	repo := NewPgNotificationRepository(pool)

	n := &model.SentNotification{
		RecipientID:    "22222222-2222-2222-2222-222222222222",
		RecipientEmail: "integration-test@example.com",
		Channel:        model.ChannelEmail,
		Subject:        "HU-NOTIF-001 integration test",
		SendStatus:     model.StatusPending,
		SourceService:  "notification-service-it",
		SourceEventID:  "33333333-3333-3333-3333-333333333333",
	}

	if err := repo.Save(ctx, n); err != nil {
		t.Fatalf("Save failed: %v", err)
	}
	if n.ID == "" {
		t.Fatal("Save did not populate ID (expected the table's DEFAULT gen_random_uuid())")
	}
	if n.CreatedAt.IsZero() {
		t.Fatal("Save did not populate CreatedAt (expected the table's DEFAULT now())")
	}

	// Verify the inserted row by reading it directly, bypassing the repo (avoids a false
	// green if Save only populated the in-memory struct without actually persisting).
	var (
		gotEmail  string
		gotStatus string
	)
	err = pool.QueryRow(ctx,
		`SELECT recipient_email, send_status FROM notification.sent_notification WHERE id = $1::uuid`,
		n.ID,
	).Scan(&gotEmail, &gotStatus)
	if err != nil {
		t.Fatalf("inserted row not found (id=%s): %v", n.ID, err)
	}
	if gotEmail != "integration-test@example.com" {
		t.Fatalf("persisted recipient_email = %q, want %q", gotEmail, "integration-test@example.com")
	}
	if gotStatus != "PENDING" {
		t.Fatalf("persisted send_status = %q, want PENDING", gotStatus)
	}

	// Cleanup deferred to FindByID test below (which re-uses the same row).
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM notification.sent_notification WHERE id = $1::uuid`, n.ID)
	})
}

// HU-NOTIF-005: verifica que FindByID lee la fila persistida por Save.
func TestPgNotificationRepository_FindByID_Integration(t *testing.T) {
	dsn := os.Getenv("NOTIFICATION_DB_DSN")
	if dsn == "" {
		t.Skip("NOTIFICATION_DB_DSN not set; skipping integration test (no default credentials in code)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := NewPool(ctx, dsn)
	if err != nil {
		t.Fatalf("failed to open pool: %v", err)
	}
	defer pool.Close()

	repo := NewPgNotificationRepository(pool)

	// Arrange: insert a row to look up.
	inserted := &model.SentNotification{
		RecipientID:    "22222222-2222-2222-2222-222222222222",
		RecipientEmail: "findbyid-it@example.com",
		Channel:        model.ChannelEmail,
		Subject:        "HU-NOTIF-005 FindByID integration test",
		SendStatus:     model.StatusPending,
	}
	if err := repo.Save(ctx, inserted); err != nil {
		t.Fatalf("Save failed: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM notification.sent_notification WHERE id = $1::uuid`, inserted.ID)
	})

	// Act E1: existing id returns the notification.
	got, err := repo.FindByID(ctx, inserted.ID)
	if err != nil {
		t.Fatalf("FindByID returned unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("FindByID returned nil for an existing id")
	}
	if got.ID != inserted.ID {
		t.Fatalf("want ID=%s, got %s", inserted.ID, got.ID)
	}
	if got.Channel != model.ChannelEmail {
		t.Fatalf("want channel=EMAIL, got %s", got.Channel)
	}
	if got.Subject != inserted.Subject {
		t.Fatalf("want subject=%q, got %q", inserted.Subject, got.Subject)
	}
	if got.SendStatus != model.StatusPending {
		t.Fatalf("want send_status=PENDING, got %s", got.SendStatus)
	}

	// Act E2: non-existent id returns (nil, nil).
	missing, err := repo.FindByID(ctx, "00000000-0000-0000-0000-000000000000")
	if err != nil {
		t.Fatalf("FindByID on missing id returned unexpected error: %v", err)
	}
	if missing != nil {
		t.Fatalf("FindByID on missing id should return nil, got %+v", missing)
	}
}
