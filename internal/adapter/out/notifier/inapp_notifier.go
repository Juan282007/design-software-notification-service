package notifier

import (
	"context"

	"github.com/code-sena/design-software-notification-service/internal/domain/model"
)

// InAppNotifier "delivers" IN_APP notifications by doing nothing beyond what
// persistence already provides: the sent_notification row, once saved, is what
// GET /notifications/{id} (HU-NOTIF-005) exposes as the in-app notification.
type InAppNotifier struct{}

func NewInAppNotifier() *InAppNotifier { return &InAppNotifier{} }

func (InAppNotifier) Send(_ context.Context, _ *model.SentNotification) error { return nil }
