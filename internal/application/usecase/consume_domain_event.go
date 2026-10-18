package usecase

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/code-sena/design-software-notification-service/internal/application/port/in"
	"github.com/code-sena/design-software-notification-service/internal/application/port/out"
	"github.com/code-sena/design-software-notification-service/internal/domain/model"
	"github.com/code-sena/design-software-notification-service/internal/domain/service"
)

const outboxEventTypeNotificationSent = "notification.notification.sent"

type recipientRef struct {
	entityType string
	entityID   string
	subject    string
}

// recipientRefFor maps a supported event_type to a recipientRef.
// scheduling.schedule.published carries instructor_ids (plural) but no single
// "affected entity". Fan-out to instructor_ids is explicitly out of scope here;
// we notify the single actor who published the schedule (published_by).
func recipientRefFor(cmd in.ConsumeDomainEventCommand) (recipientRef, error) {
	switch cmd.EventType {
	case "monitoring.alert.triggered":
		entityType, _ := cmd.Payload["affected_entity_type"].(string)
		entityID, _ := cmd.Payload["affected_entity_id"].(string)
		if entityType == "" || entityID == "" {
			return recipientRef{}, fmt.Errorf("monitoring.alert.triggered payload missing affected_entity_type/affected_entity_id")
		}
		alertType, _ := cmd.Payload["alert_type_code"].(string)
		return recipientRef{
			entityType: entityType,
			entityID:   entityID,
			subject:    fmt.Sprintf("Alert triggered: %s", alertType),
		}, nil

	case "scheduling.schedule.published":
		publishedBy, _ := cmd.Payload["published_by"].(string)
		if publishedBy == "" {
			return recipientRef{}, fmt.Errorf("scheduling.schedule.published payload missing published_by")
		}
		return recipientRef{
			entityType: "Instructor",
			entityID:   publishedBy,
			subject:    "Schedule published",
		}, nil

	default:
		// Unsupported event_type is a permanent failure — the AMQP consumer must not retry.
		return recipientRef{}, fmt.Errorf("unsupported event_type %s: %w", cmd.EventType, model.ErrPermanentFailure)
	}
}

// templateCodeFor maps a supported event_type to its notification_template code.
// Returns empty string when the event has no associated template.
func templateCodeFor(eventType string) string {
	switch eventType {
	case "monitoring.alert.triggered":
		return "ALERT_TRIGGERED"
	case "scheduling.schedule.published":
		return "SCHEDULE_PUBLISHED"
	default:
		return ""
	}
}

// templateVarsFor extracts the template variables from an event payload.
// Keys must match the {{placeholder}} names defined in notification_template.
func templateVarsFor(eventType string, payload map[string]interface{}) map[string]string {
	str := func(v interface{}) string { s, _ := v.(string); return s }
	switch eventType {
	case "monitoring.alert.triggered":
		return map[string]string{
			"alert_type": str(payload["alert_type_code"]),
			"ficha":      str(payload["affected_entity_id"]),
		}
	case "scheduling.schedule.published":
		return map[string]string{
			"schedule_name": str(payload["schedule_name"]),
			"ficha":         str(payload["ficha"]),
		}
	default:
		return nil
	}
}

type outboundNotificationSentPayload struct {
	NotificationID string     `json:"notification_id"`
	RecipientID    string     `json:"recipient_id"`
	Channel        string     `json:"channel"`
	SentAt         *time.Time `json:"sent_at"`
	// TraceParent carries the inbound event's trace context (HU-NOTIF-007) so
	// OutboxRelay can continue the same trace when it eventually publishes. Plain
	// pass-through string; see ConsumeDomainEventCommand.TraceParent.
	TraceParent string `json:"trace_parent,omitempty"`
}

// ConsumeDomainEvent implements HU-NOTIF-002 + HU-NOTIF-006: for a supported inbound
// event, resolves a recipient, renders the associated template (if any), attempts
// delivery, persists the resulting SentNotification, and stages notification.sent in
// the Outbox atomically on success. Idempotency is enforced by the repository/DB.
type ConsumeDomainEvent struct {
	resolver out.RecipientResolver
	notifier out.Notifier
	repo     out.NotificationRepository
	tmplRepo out.TemplateRepository
}

func NewConsumeDomainEvent(
	resolver out.RecipientResolver,
	notifier out.Notifier,
	repo out.NotificationRepository,
	tmplRepo out.TemplateRepository,
) *ConsumeDomainEvent {
	return &ConsumeDomainEvent{resolver: resolver, notifier: notifier, repo: repo, tmplRepo: tmplRepo}
}

func (c *ConsumeDomainEvent) Handle(ctx context.Context, cmd in.ConsumeDomainEventCommand) error {
	// HU-NOTIF-004 E3: skip everything if this event_id was already fully processed.
	// This prevents duplicate sends on AMQP redelivery (crash between Send and Ack).
	if cmd.EventID != "" {
		exists, err := c.repo.ExistsBySourceEventID(ctx, cmd.EventID)
		if err != nil {
			return fmt.Errorf("check idempotency for %s: %w", cmd.EventID, err)
		}
		if exists {
			return nil
		}
	}

	ref, err := recipientRefFor(cmd)
	if err != nil {
		return fmt.Errorf("map event %s: %w", cmd.EventType, err)
	}

	recipient, err := c.resolver.Resolve(ctx, ref.entityType, ref.entityID)
	if err != nil {
		return fmt.Errorf("resolve recipient: %w", err)
	}

	now := time.Now()
	n := &model.SentNotification{
		ID:             uuid.NewString(),
		RecipientID:    recipient.ID,
		RecipientEmail: recipient.Email,
		Channel:        model.ChannelEmail,
		Subject:        ref.subject,
		SourceService:  cmd.SourceService,
		SourceEventID:  cmd.EventID,
		CreatedAt:      now,
	}

	// HU-NOTIF-006: render template if one is registered for this event_type.
	if code := templateCodeFor(cmd.EventType); code != "" {
		if tmpl, tErr := c.tmplRepo.FindByCode(ctx, code); tErr == nil && tmpl != nil && tmpl.IsActive {
			vars := templateVarsFor(cmd.EventType, cmd.Payload)
			n.Subject = service.Render(tmpl.SubjectTemplate, vars)
			n.BodySummary = service.Render(tmpl.BodyTemplate, vars)
			n.TemplateID = tmpl.ID
		}
		// template not found or inactive: keep the hardcoded subject from recipientRefFor
	}

	if sendErr := c.notifier.Send(ctx, n); sendErr != nil {
		n.SendStatus = model.StatusFailed
		n.FailureReason = sendErr.Error()
		// Persist FAILED with upsert so concurrent retry attempts update instead of
		// inserting duplicates (unique constraint on source_event_id).
		if pErr := c.repo.UpsertFailed(ctx, n); pErr != nil {
			return fmt.Errorf("persist failure for %s: %w", cmd.EventID, pErr)
		}
		// Return the delivery error so the AMQP consumer retries (or routes to DLQ after
		// exhausting max attempts). Returning nil here was the original bug — it caused
		// the consumer to Ack regardless of send failure (HU-NOTIF-004 E1/E2 fix).
		return fmt.Errorf("notify: %w", sendErr)
	}

	n.SendStatus = model.StatusSent
	sentAt := time.Now()
	n.SentAt = &sentAt

	payload, mErr := json.Marshal(outboundNotificationSentPayload{
		NotificationID: n.ID,
		RecipientID:    n.RecipientID,
		Channel:        string(n.Channel),
		SentAt:         n.SentAt,
		TraceParent:    cmd.TraceParent,
	})
	if mErr != nil {
		return fmt.Errorf("marshal outbox payload: %w", mErr)
	}
	evt := &model.OutboxEvent{
		ID:        uuid.NewString(),
		EventID:   uuid.NewString(),
		EventType: outboxEventTypeNotificationSent,
		Payload:   payload,
		CreatedAt: now,
	}

	if _, err := c.repo.SaveWithOutbox(ctx, n, evt); err != nil {
		return fmt.Errorf("persist notification: %w", err)
	}
	return nil
}
