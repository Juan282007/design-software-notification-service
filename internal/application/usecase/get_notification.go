package usecase

import (
	"context"

	"github.com/code-sena/design-software-notification-service/internal/application/port/in"
	"github.com/code-sena/design-software-notification-service/internal/application/port/out"
	"github.com/code-sena/design-software-notification-service/internal/domain/model"
)

type GetNotification struct {
	repo out.NotificationRepository
}

func NewGetNotification(repo out.NotificationRepository) *GetNotification {
	return &GetNotification{repo: repo}
}

func (g *GetNotification) Handle(ctx context.Context, q in.GetNotificationQuery) (*model.SentNotification, error) {
	n, err := g.repo.FindByID(ctx, q.ID)
	if err != nil {
		return nil, err
	}
	if n == nil {
		return nil, model.ErrNotFound
	}
	return n, nil
}
