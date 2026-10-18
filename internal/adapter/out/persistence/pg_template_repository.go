package persistence

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code-sena/design-software-notification-service/internal/domain/model"
)

// PgTemplateRepository: adapter de salida para notification.notification_template.
type PgTemplateRepository struct {
	pool *pgxpool.Pool
}

func NewPgTemplateRepository(pool *pgxpool.Pool) *PgTemplateRepository {
	return &PgTemplateRepository{pool: pool}
}

// FindByCode returns the active template matching code, or nil when not found.
func (r *PgTemplateRepository) FindByCode(ctx context.Context, code string) (*model.NotificationTemplate, error) {
	const q = `
		SELECT id::text, code, channel, subject_template, body_template, is_active, created_at, updated_at
		FROM notification.notification_template
		WHERE code = $1`

	var t model.NotificationTemplate
	var channel string
	err := r.pool.QueryRow(ctx, q, code).Scan(
		&t.ID, &t.Code, &channel,
		&t.SubjectTemplate, &t.BodyTemplate,
		&t.IsActive, &t.CreatedAt, &t.UpdatedAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	t.Channel = model.Channel(channel)
	return &t, nil
}
