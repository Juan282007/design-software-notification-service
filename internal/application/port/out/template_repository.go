package out

import (
	"context"

	"github.com/code-sena/design-software-notification-service/internal/domain/model"
)

// TemplateRepository: port for looking up notification templates by code.
type TemplateRepository interface {
	// FindByCode returns the template with the given code, or nil when no row matches.
	// A non-nil error signals an infrastructure failure (e.g. DB unreachable).
	FindByCode(ctx context.Context, code string) (*model.NotificationTemplate, error)
}
