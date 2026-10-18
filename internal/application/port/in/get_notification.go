package in

import (
	"context"

	"github.com/code-sena/design-software-notification-service/internal/domain/model"
)

type GetNotificationQuery struct {
	ID string
}

// GetNotificationUseCase: puerto de entrada para consultar una notificacion enviada (HU-NOTIF-005).
type GetNotificationUseCase interface {
	Handle(ctx context.Context, q GetNotificationQuery) (*model.SentNotification, error)
}
