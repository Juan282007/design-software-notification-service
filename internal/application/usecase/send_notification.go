package usecase

import (
	"context"
	"time"

	"github.com/code-sena/design-software-notification-service/internal/application/port/in"
	"github.com/code-sena/design-software-notification-service/internal/application/port/out"
	"github.com/code-sena/design-software-notification-service/internal/domain/model"
	"github.com/code-sena/design-software-notification-service/internal/domain/service"
)

type SendNotification struct {
	repo     out.NotificationRepository
	tmplRepo out.TemplateRepository
	notifier out.Notifier
}

func NewSendNotification(repo out.NotificationRepository, tmplRepo out.TemplateRepository, notifier out.Notifier) *SendNotification {
	return &SendNotification{repo: repo, tmplRepo: tmplRepo, notifier: notifier}
}

// Handle validates and delivers a notification through the configured channel adapter.
// Template rendering (HU-006) runs before delivery; if the template is missing or inactive
// the explicit cmd.Subject is used. On delivery failure the record is still persisted with
// send_status=FAILED so no notification is lost (HU-003 E3).
func (s *SendNotification) Handle(ctx context.Context, cmd in.SendNotificationCommand) (*model.SentNotification, error) {
	n := &model.SentNotification{
		RecipientID:    cmd.RecipientID,
		RecipientEmail: cmd.RecipientEmail,
		Channel:        cmd.Channel,
		Subject:        cmd.Subject,
		SourceService:  cmd.SourceService,
		SourceEventID:  cmd.SourceEventID,
		CreatedAt:      time.Now(),
	}

	if cmd.TemplateCode != "" {
		if tmpl, err := s.tmplRepo.FindByCode(ctx, cmd.TemplateCode); err == nil && tmpl != nil && tmpl.IsActive {
			n.Subject = service.Render(tmpl.SubjectTemplate, cmd.TemplateVars)
			n.BodySummary = service.Render(tmpl.BodyTemplate, cmd.TemplateVars)
			n.TemplateID = tmpl.ID
		}
	}

	if sendErr := s.notifier.Send(ctx, n); sendErr != nil {
		n.SendStatus = model.StatusFailed
		n.FailureReason = sendErr.Error()
	} else {
		n.SendStatus = model.StatusSent
		now := time.Now()
		n.SentAt = &now
	}

	if err := s.repo.Save(ctx, n); err != nil {
		return nil, err
	}
	return n, nil
}
