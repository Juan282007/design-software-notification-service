package out

import (
	"context"

	"github.com/code-sena/design-software-notification-service/internal/domain/model"
)

// RecipientResolver resolves who should receive a notification for a given domain
// entity referenced by an inbound event (e.g. an Instructor, a Learner, a Ficha).
//
// TODO: the real implementation should call actors-service (Instructor/Learner ->
// email) once that repo exists (see ADR-006; Java/Spring, not built yet). Today only
// a stub adapter is wired (adapter/out/client), returning a fixed recipient so the
// delivery pipeline is exercisable end-to-end (including MailHog) without that
// dependency. See HU-NOTIF-002 body for the deferred-dependency note.
type RecipientResolver interface {
	Resolve(ctx context.Context, entityType, entityID string) (model.Recipient, error)
}
