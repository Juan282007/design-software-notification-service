package notifier

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/code-sena/design-software-notification-service/internal/domain/model"
	otelplatform "github.com/code-sena/design-software-notification-service/internal/platform/otel"
)

// CompositeNotifier dispatches Send by channel so the use case depends on a single
// out.Notifier port regardless of how many channels exist. It also records the
// notifications-delivered metric (HU-NOTIF-007): this is the one place that knows both
// the channel and the real delivery outcome, so it's a better fit than the use case
// (which would need an OTel import to do the same thing).
type CompositeNotifier struct {
	email   *SMTPNotifier
	inApp   *InAppNotifier
	metrics *otelplatform.Metrics // optional; nil is a valid no-metrics mode (e.g. tests)
}

func NewCompositeNotifier(email *SMTPNotifier, inApp *InAppNotifier, metrics *otelplatform.Metrics) *CompositeNotifier {
	return &CompositeNotifier{email: email, inApp: inApp, metrics: metrics}
}

func (c *CompositeNotifier) Send(ctx context.Context, n *model.SentNotification) error {
	var err error
	switch n.Channel {
	case model.ChannelEmail:
		err = c.email.Send(ctx, n)
	case model.ChannelInApp:
		err = c.inApp.Send(ctx, n)
	default:
		err = fmt.Errorf("unsupported channel: %s", n.Channel)
	}

	if c.metrics != nil {
		status := "SENT"
		if err != nil {
			status = "FAILED"
		}
		c.metrics.NotificationsDelivered.Add(ctx, 1, metric.WithAttributes(
			attribute.String("channel", string(n.Channel)),
			attribute.String("status", status),
		))
	}
	return err
}
