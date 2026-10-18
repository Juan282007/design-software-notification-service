package client

import (
	"context"

	"github.com/code-sena/design-software-notification-service/internal/domain/model"
)

// StubRecipientResolver is a placeholder for the real actors-service client (that repo
// does not exist yet, see out.RecipientResolver). It returns a fixed, configurable
// recipient email so the delivery pipeline -- including a real MailHog-based
// verification -- is exercisable end to end. The recipient's ID passes through the
// entity ID from the event, matching the real resolver's eventual contract.
type StubRecipientResolver struct {
	email string
}

func NewStubRecipientResolver(email string) *StubRecipientResolver {
	return &StubRecipientResolver{email: email}
}

func (r *StubRecipientResolver) Resolve(_ context.Context, _ string, entityID string) (model.Recipient, error) {
	return model.Recipient{ID: entityID, Email: r.email}, nil
}
