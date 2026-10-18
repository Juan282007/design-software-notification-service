package in

import (
	"context"

	"github.com/code-sena/design-software-notification-service/internal/domain/model"
)

type SendNotificationCommand struct {
	RecipientID    string
	RecipientEmail string
	Channel        model.Channel
	Subject        string
	TemplateCode   string            // optional: renders subject/body from notification_template
	TemplateVars   map[string]string // optional: variables to substitute in the template
	SourceService  string
	SourceEventID  string
}

// SendNotificationUseCase: puerto de entrada (lo implementa la capa de aplicacion).
type SendNotificationUseCase interface {
	Handle(ctx context.Context, cmd SendNotificationCommand) (*model.SentNotification, error)
}
