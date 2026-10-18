package out

import (
	"context"

	"github.com/code-sena/design-software-notification-service/internal/domain/model"
)

// Notifier performs the actual delivery of a notification through its channel
// (EMAIL, IN_APP). An error means delivery failed; the caller maps that to
// send_status=FAILED without aborting persistence.
type Notifier interface {
	Send(ctx context.Context, n *model.SentNotification) error
}
