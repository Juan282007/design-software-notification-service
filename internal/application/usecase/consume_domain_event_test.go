package usecase

import (
	"context"
	"errors"
	"testing"

	"github.com/code-sena/design-software-notification-service/internal/application/port/in"
	"github.com/code-sena/design-software-notification-service/internal/domain/model"
)

type fakeResolver struct {
	recipient model.Recipient
	err       error
}

func (f *fakeResolver) Resolve(_ context.Context, _, _ string) (model.Recipient, error) {
	return f.recipient, f.err
}

type fakeNotifier struct {
	called int
	err    error
}

func (f *fakeNotifier) Send(_ context.Context, _ *model.SentNotification) error {
	f.called++
	return f.err
}

func alertCmd(eventID string) in.ConsumeDomainEventCommand {
	return in.ConsumeDomainEventCommand{
		EventID:       eventID,
		EventType:     "monitoring.alert.triggered",
		SourceService: "monitoring-service",
		Payload: map[string]interface{}{
			"affected_entity_type": "Learner",
			"affected_entity_id":   "l-1",
			"alert_type_code":      "LOW_ATTENDANCE",
		},
	}
}

func noTemplateFakeRepo() *fakeTemplateRepo { return &fakeTemplateRepo{tmpl: nil} }

// E1: delivery succeeds → SENT, outbox event staged.
func TestConsumeDomainEvent_DeliversAndStagesOutbox(t *testing.T) {
	repo := &fakeRepo{}
	resolver := &fakeResolver{recipient: model.Recipient{ID: "l-1", Email: "dev-notifications@sena.local"}}
	notifier := &fakeNotifier{}
	uc := NewConsumeDomainEvent(resolver, notifier, repo, noTemplateFakeRepo())

	if err := uc.Handle(context.Background(), alertCmd("evt-1")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if repo.outboxSaved != 1 {
		t.Fatalf("want 1 SaveWithOutbox call, got %d", repo.outboxSaved)
	}
	if repo.lastNotification.SendStatus != model.StatusSent {
		t.Fatalf("want SENT, got %s", repo.lastNotification.SendStatus)
	}
	if repo.lastNotification.SourceEventID != "evt-1" {
		t.Fatalf("source_event_id not propagated: %+v", repo.lastNotification)
	}
	if repo.lastOutboxEvent == nil {
		t.Fatal("expected an outbox event to be staged on successful delivery")
	}
	if repo.lastOutboxEvent.EventType != outboxEventTypeNotificationSent {
		t.Fatalf("want outbox event_type %q, got %q", outboxEventTypeNotificationSent, repo.lastOutboxEvent.EventType)
	}
}

// dedup: already processed → clean no-op.
func TestConsumeDomainEvent_AlreadyProcessed_IsNotAnError(t *testing.T) {
	repo := &fakeRepo{alreadyProcessed: true}
	resolver := &fakeResolver{recipient: model.Recipient{ID: "l-1", Email: "dev-notifications@sena.local"}}
	notifier := &fakeNotifier{}
	uc := NewConsumeDomainEvent(resolver, notifier, repo, noTemplateFakeRepo())

	if err := uc.Handle(context.Background(), alertCmd("evt-1")); err != nil {
		t.Fatalf("duplicate event should not surface as an error, got: %v", err)
	}
}

// HU-NOTIF-004 E1/E2 fix: delivery failure → use case returns error (so consumer retries),
// persists FAILED via UpsertFailed, and does NOT stage an outbox event.
func TestConsumeDomainEvent_NotifierFails_ReturnsErrorAndUpsertsFailed(t *testing.T) {
	repo := &fakeRepo{}
	resolver := &fakeResolver{recipient: model.Recipient{ID: "l-1", Email: "dev-notifications@sena.local"}}
	notifier := &fakeNotifier{err: errors.New("smtp: connection refused")}
	uc := NewConsumeDomainEvent(resolver, notifier, repo, noTemplateFakeRepo())

	err := uc.Handle(context.Background(), alertCmd("evt-2"))
	if err == nil {
		t.Fatal("delivery failure must return a non-nil error so the consumer retries (was the root bug)")
	}
	if repo.upsertFailedCalled != 1 {
		t.Fatalf("want 1 UpsertFailed call, got %d", repo.upsertFailedCalled)
	}
	if repo.lastFailed == nil || repo.lastFailed.SendStatus != model.StatusFailed {
		t.Fatalf("want FAILED record upserted, got: %+v", repo.lastFailed)
	}
	if repo.lastFailed.FailureReason == "" {
		t.Fatal("FailureReason must be populated")
	}
	if repo.outboxSaved != 0 {
		t.Fatal("failed delivery must not stage an outbox event")
	}
}

// unsupported event_type → rejected before touching repo.
func TestConsumeDomainEvent_UnsupportedEventType_ReturnsError(t *testing.T) {
	repo := &fakeRepo{}
	uc := NewConsumeDomainEvent(&fakeResolver{}, &fakeNotifier{}, repo, noTemplateFakeRepo())

	cmd := in.ConsumeDomainEventCommand{EventID: "evt-3", EventType: "academic.enrollment_ficha.status_changed"}
	if err := uc.Handle(context.Background(), cmd); err == nil {
		t.Fatal("expected an error for an unsupported event_type")
	}
	if repo.outboxSaved != 0 {
		t.Fatal("repo should not be called for an unsupported event_type")
	}
}

// scheduling.schedule.published maps to published_by.
func TestConsumeDomainEvent_SchedulePublished_NotifiesPublisher(t *testing.T) {
	repo := &fakeRepo{}
	resolver := &fakeResolver{recipient: model.Recipient{ID: "instructor-1", Email: "dev-notifications@sena.local"}}
	notifier := &fakeNotifier{}
	uc := NewConsumeDomainEvent(resolver, notifier, repo, noTemplateFakeRepo())

	cmd := in.ConsumeDomainEventCommand{
		EventID:       "evt-4",
		EventType:     "scheduling.schedule.published",
		SourceService: "scheduling-service",
		Payload:       map[string]interface{}{"published_by": "instructor-1", "schedule_id": "sch-1"},
	}
	if err := uc.Handle(context.Background(), cmd); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if repo.lastNotification.RecipientID != "instructor-1" {
		t.Fatalf("want recipient instructor-1, got %s", repo.lastNotification.RecipientID)
	}
}

// HU-NOTIF-006 E1: active template → subject + body rendered, template_id stored.
func TestConsumeDomainEvent_ActiveTemplate_RendersSubjectAndBody(t *testing.T) {
	repo := &fakeRepo{}
	resolver := &fakeResolver{recipient: model.Recipient{ID: "l-1", Email: "dev-notifications@sena.local"}}
	notifier := &fakeNotifier{}
	tmpl := &model.NotificationTemplate{
		ID:              "tmpl-uuid-alert",
		Code:            "ALERT_TRIGGERED",
		IsActive:        true,
		SubjectTemplate: "Alerta: {{alert_type}}",
		BodyTemplate:    "Se genero una alerta {{alert_type}} en la ficha {{ficha}}.",
	}
	uc := NewConsumeDomainEvent(resolver, notifier, repo, &fakeTemplateRepo{tmpl: tmpl})

	if err := uc.Handle(context.Background(), alertCmd("evt-5")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if repo.lastNotification.Subject != "Alerta: LOW_ATTENDANCE" {
		t.Fatalf("subject not rendered: %q", repo.lastNotification.Subject)
	}
	if repo.lastNotification.BodySummary != "Se genero una alerta LOW_ATTENDANCE en la ficha l-1." {
		t.Fatalf("body not rendered: %q", repo.lastNotification.BodySummary)
	}
	if repo.lastNotification.TemplateID != "tmpl-uuid-alert" {
		t.Fatalf("template_id not stored: %q", repo.lastNotification.TemplateID)
	}
}

// HU-NOTIF-006 E3: inactive template → falls back to hardcoded subject.
func TestConsumeDomainEvent_InactiveTemplate_FallsBackToHardcodedSubject(t *testing.T) {
	repo := &fakeRepo{}
	resolver := &fakeResolver{recipient: model.Recipient{ID: "l-1", Email: "dev-notifications@sena.local"}}
	notifier := &fakeNotifier{}
	inactiveTmpl := &model.NotificationTemplate{
		ID:       "tmpl-uuid-inactive",
		Code:     "ALERT_TRIGGERED",
		IsActive: false,
	}
	uc := NewConsumeDomainEvent(resolver, notifier, repo, &fakeTemplateRepo{tmpl: inactiveTmpl})

	if err := uc.Handle(context.Background(), alertCmd("evt-6")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if repo.lastNotification.TemplateID != "" {
		t.Fatalf("template_id should be empty for inactive template, got %q", repo.lastNotification.TemplateID)
	}
}

// HU-NOTIF-004 E3: event_id already in DB → notifier is never called (prevents duplicate
// delivery on AMQP redelivery after a consumer crash between Send and Ack).
func TestConsumeDomainEvent_AlreadyExists_SkipsNotifierAndReturnsNil(t *testing.T) {
	repo := &fakeRepo{eventExists: true}
	notifier := &fakeNotifier{}
	uc := NewConsumeDomainEvent(&fakeResolver{}, notifier, repo, noTemplateFakeRepo())

	if err := uc.Handle(context.Background(), alertCmd("evt-dup")); err != nil {
		t.Fatalf("already-exists should be a clean no-op, got: %v", err)
	}
	if notifier.called != 0 {
		t.Fatal("notifier must not be called when event_id is already processed")
	}
	if repo.outboxSaved != 0 {
		t.Fatal("SaveWithOutbox must not be called when event_id is already processed")
	}
}

// HU-NOTIF-004 E3 (fix): a FAILED record from a prior attempt must NOT short-circuit the
// retry — only a SENT record counts as "already processed". fakeRepo.eventExists=false
// simulates ExistsBySourceEventID returning false for a FAILED row (new SQL filters SENT only).
func TestConsumeDomainEvent_FailedRecordExists_DoesNotSkipRetry(t *testing.T) {
	repo := &fakeRepo{eventExists: false} // FAILED exists in DB, but ExistsBySourceEventID returns false
	notifier := &fakeNotifier{err: errors.New("smtp: still down")}
	uc := NewConsumeDomainEvent(&fakeResolver{recipient: model.Recipient{ID: "l-1", Email: "t@t.co"}}, notifier, repo, noTemplateFakeRepo())

	err := uc.Handle(context.Background(), alertCmd("evt-retry-fail"))
	if err == nil {
		t.Fatal("expected error so consumer can retry")
	}
	if notifier.called == 0 {
		t.Fatal("notifier must be called when no SENT record exists (retry path)")
	}
	if repo.upsertFailedCalled != 1 {
		t.Fatalf("want 1 UpsertFailed call on retry, got %d", repo.upsertFailedCalled)
	}
}

// HU-NOTIF-004 E1: unsupported event_type returns ErrPermanentFailure (no retry).
func TestConsumeDomainEvent_UnsupportedEventType_ReturnsPermanentFailure(t *testing.T) {
	uc := NewConsumeDomainEvent(&fakeResolver{}, &fakeNotifier{}, &fakeRepo{}, noTemplateFakeRepo())

	cmd := in.ConsumeDomainEventCommand{EventID: "evt-perm", EventType: "academic.unknown"}
	err := uc.Handle(context.Background(), cmd)
	if err == nil {
		t.Fatal("expected an error for unsupported event_type")
	}
	if !errors.Is(err, model.ErrPermanentFailure) {
		t.Fatalf("expected ErrPermanentFailure, got: %v", err)
	}
}
