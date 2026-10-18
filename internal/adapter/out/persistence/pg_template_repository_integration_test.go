package persistence

import (
	"context"
	"os"
	"testing"
)

func TestPgTemplateRepository_FindByCode_Integration(t *testing.T) {
	dsn := os.Getenv("NOTIFICATION_DB_DSN")
	if dsn == "" {
		t.Skip("NOTIFICATION_DB_DSN not set — skipping integration test")
	}

	ctx := context.Background()
	pool, err := NewPool(ctx, dsn)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	defer pool.Close()

	repo := NewPgTemplateRepository(pool)

	t.Run("active template found", func(t *testing.T) {
		tmpl, err := repo.FindByCode(ctx, "SCHEDULE_PUBLISHED")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if tmpl == nil {
			t.Fatal("expected template SCHEDULE_PUBLISHED to exist (seed)")
		}
		if !tmpl.IsActive {
			t.Fatal("expected SCHEDULE_PUBLISHED to be active")
		}
		if tmpl.SubjectTemplate == "" || tmpl.BodyTemplate == "" {
			t.Fatalf("template templates empty: subject=%q body=%q", tmpl.SubjectTemplate, tmpl.BodyTemplate)
		}
	})

	t.Run("nonexistent code returns nil", func(t *testing.T) {
		tmpl, err := repo.FindByCode(ctx, "NONEXISTENT_CODE_12345")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if tmpl != nil {
			t.Fatalf("expected nil for nonexistent code, got %+v", tmpl)
		}
	})
}
