package usecase

import (
	"context"
	"errors"
	"testing"

	"github.com/code-sena/design-software-notification-service/internal/application/port/in"
	"github.com/code-sena/design-software-notification-service/internal/domain/model"
)

// fakeRepo is shared by all usecase tests (Save, SaveWithOutbox, FindByID, ExistsBySourceEventID, UpsertFailed).
type fakeRepo struct {
	saved   int
	lastArg *model.SentNotification
	err     error

	outboxSaved      int
	lastNotification *model.SentNotification
	lastOutboxEvent  *model.OutboxEvent
	alreadyProcessed bool

	// FindByID-specific
	foundByID *model.SentNotification
	findErr   error

	// ExistsBySourceEventID-specific (HU-NOTIF-004 E3)
	eventExists    bool
	eventExistsErr error
	existsCalled   bool

	// UpsertFailed-specific (HU-NOTIF-004 E1/E2 fix)
	upsertFailedCalled int
	upsertFailedErr    error
	lastFailed         *model.SentNotification
}

func (f *fakeRepo) Save(_ context.Context, n *model.SentNotification) error {
	f.saved++
	f.lastArg = n
	return f.err
}

func (f *fakeRepo) SaveWithOutbox(_ context.Context, n *model.SentNotification, evt *model.OutboxEvent) (bool, error) {
	if f.err != nil {
		return false, f.err
	}
	f.outboxSaved++
	f.lastNotification = n
	f.lastOutboxEvent = evt
	return f.alreadyProcessed, nil
}

func (f *fakeRepo) FindByID(_ context.Context, _ string) (*model.SentNotification, error) {
	return f.foundByID, f.findErr
}

func (f *fakeRepo) ExistsBySourceEventID(_ context.Context, _ string) (bool, error) {
	f.existsCalled = true
	return f.eventExists, f.eventExistsErr
}

func (f *fakeRepo) UpsertFailed(_ context.Context, n *model.SentNotification) error {
	f.upsertFailedCalled++
	f.lastFailed = n
	return f.upsertFailedErr
}

// fakeTemplateRepo allows controlling FindByCode results in tests.
type fakeTemplateRepo struct {
	tmpl *model.NotificationTemplate
	err  error
}

func (f *fakeTemplateRepo) FindByCode(_ context.Context, _ string) (*model.NotificationTemplate, error) {
	return f.tmpl, f.err
}

// --- SendNotification tests ---

// HU-003 E1: channel=EMAIL, notifier succeeds → send_status=SENT, sent_at set, persisted once.
func TestSendNotification_Email_SetsSent(t *testing.T) {
	repo := &fakeRepo{}
	notify := &fakeNotifier{}
	uc := NewSendNotification(repo, &fakeTemplateRepo{}, notify)

	n, err := uc.Handle(context.Background(), in.SendNotificationCommand{
		RecipientID:    "r1",
		RecipientEmail: "a@b.co",
		Channel:        model.ChannelEmail,
		Subject:        "hi",
		SourceService:  "monitoring",
		SourceEventID:  "evt-1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n.SendStatus != model.StatusSent {
		t.Fatalf("E1: want SENT, got %s", n.SendStatus)
	}
	if n.SentAt == nil {
		t.Fatal("E1: want sent_at set, got nil")
	}
	if n.FailureReason != "" {
		t.Fatalf("E1: want empty failure_reason, got %q", n.FailureReason)
	}
	if repo.saved != 1 {
		t.Fatalf("want 1 save, got %d", repo.saved)
	}
	if notify.called != 1 {
		t.Fatalf("want notifier called once, got %d", notify.called)
	}
	if repo.lastArg.SourceService != "monitoring" || repo.lastArg.SourceEventID != "evt-1" {
		t.Fatalf("source_service/source_event_id not propagated: %+v", repo.lastArg)
	}
}

// HU-003 E2: channel=IN_APP, notifier succeeds → send_status=SENT, persisted once.
func TestSendNotification_InApp_SetsSent(t *testing.T) {
	repo := &fakeRepo{}
	notify := &fakeNotifier{}
	uc := NewSendNotification(repo, &fakeTemplateRepo{}, notify)

	n, err := uc.Handle(context.Background(), in.SendNotificationCommand{
		RecipientID:    "r2",
		RecipientEmail: "b@c.co",
		Channel:        model.ChannelInApp,
		Subject:        "in-app message",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n.SendStatus != model.StatusSent {
		t.Fatalf("E2: want SENT, got %s", n.SendStatus)
	}
	if n.SentAt == nil {
		t.Fatal("E2: want sent_at set, got nil")
	}
	if repo.saved != 1 {
		t.Fatalf("want 1 save, got %d", repo.saved)
	}
}

// HU-003 E3: notifier fails → send_status=FAILED, failure_reason populated, record still persisted.
func TestSendNotification_NotifierFails_SetsFailed(t *testing.T) {
	repo := &fakeRepo{}
	notify := &fakeNotifier{err: errors.New("smtp: connection refused")}
	uc := NewSendNotification(repo, &fakeTemplateRepo{}, notify)

	n, err := uc.Handle(context.Background(), in.SendNotificationCommand{
		RecipientID:    "r3",
		RecipientEmail: "c@d.co",
		Channel:        model.ChannelEmail,
		Subject:        "alert",
	})
	if err != nil {
		t.Fatalf("E3: use case must not error when notifier fails; got: %v", err)
	}
	if n.SendStatus != model.StatusFailed {
		t.Fatalf("E3: want FAILED, got %s", n.SendStatus)
	}
	if n.FailureReason == "" {
		t.Fatal("E3: want failure_reason populated, got empty")
	}
	if n.SentAt != nil {
		t.Fatalf("E3: want sent_at nil on failure, got %v", n.SentAt)
	}
	if repo.saved != 1 {
		t.Fatalf("E3: record must still be persisted; want 1 save, got %d", repo.saved)
	}
}

func TestSendNotification_PropagatesRepoError(t *testing.T) {
	repo := &fakeRepo{err: errors.New("db down")}
	uc := NewSendNotification(repo, &fakeTemplateRepo{}, &fakeNotifier{})

	_, err := uc.Handle(context.Background(), in.SendNotificationCommand{
		RecipientID:    "r1",
		RecipientEmail: "a@b.co",
		Channel:        model.ChannelEmail,
		Subject:        "hi",
	})
	if err == nil {
		t.Fatal("esperaba error del repositorio, obtuve nil")
	}
}

// active template renders subject + body and stores template_id.
func TestSendNotification_ActiveTemplate_RendersAndStoresTemplateID(t *testing.T) {
	repo := &fakeRepo{}
	tmpl := &model.NotificationTemplate{
		ID:              "tmpl-uuid-1",
		Code:            "SCHEDULE_PUBLISHED",
		Channel:         model.ChannelEmail,
		SubjectTemplate: "Tu horario {{schedule_name}} fue publicado",
		BodyTemplate:    "El horario {{schedule_name}} de la ficha {{ficha}} ha sido publicado.",
		IsActive:        true,
	}
	uc := NewSendNotification(repo, &fakeTemplateRepo{tmpl: tmpl}, &fakeNotifier{})

	n, err := uc.Handle(context.Background(), in.SendNotificationCommand{
		RecipientID:    "r1",
		RecipientEmail: "a@b.co",
		Channel:        model.ChannelEmail,
		Subject:        "fallback subject",
		TemplateCode:   "SCHEDULE_PUBLISHED",
		TemplateVars:   map[string]string{"schedule_name": "Ene-2026", "ficha": "2850621"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n.Subject != "Tu horario Ene-2026 fue publicado" {
		t.Fatalf("subject not rendered: %q", n.Subject)
	}
	if n.BodySummary != "El horario Ene-2026 de la ficha 2850621 ha sido publicado." {
		t.Fatalf("body not rendered: %q", n.BodySummary)
	}
	if n.TemplateID != "tmpl-uuid-1" {
		t.Fatalf("template_id not stored: %q", n.TemplateID)
	}
}

// template_code not found → falls back to explicit subject.
func TestSendNotification_TemplateNotFound_FallsBackToExplicitSubject(t *testing.T) {
	repo := &fakeRepo{}
	uc := NewSendNotification(repo, &fakeTemplateRepo{tmpl: nil}, &fakeNotifier{})

	n, err := uc.Handle(context.Background(), in.SendNotificationCommand{
		RecipientID:    "r1",
		RecipientEmail: "a@b.co",
		Channel:        model.ChannelEmail,
		Subject:        "explicit subject",
		TemplateCode:   "NONEXISTENT",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n.Subject != "explicit subject" {
		t.Fatalf("want fallback subject, got %q", n.Subject)
	}
	if n.TemplateID != "" {
		t.Fatalf("template_id should be empty when template not found, got %q", n.TemplateID)
	}
}

// inactive template → falls back to explicit subject.
func TestSendNotification_InactiveTemplate_FallsBackToExplicitSubject(t *testing.T) {
	repo := &fakeRepo{}
	inactiveTmpl := &model.NotificationTemplate{
		ID:              "tmpl-uuid-2",
		Code:            "ALERT_TRIGGERED",
		IsActive:        false,
		SubjectTemplate: "should not be used",
	}
	uc := NewSendNotification(repo, &fakeTemplateRepo{tmpl: inactiveTmpl}, &fakeNotifier{})

	n, err := uc.Handle(context.Background(), in.SendNotificationCommand{
		RecipientID:    "r1",
		RecipientEmail: "a@b.co",
		Channel:        model.ChannelEmail,
		Subject:        "explicit subject",
		TemplateCode:   "ALERT_TRIGGERED",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n.Subject != "explicit subject" {
		t.Fatalf("want fallback subject for inactive template, got %q", n.Subject)
	}
	if n.TemplateID != "" {
		t.Fatalf("template_id should be empty for inactive template, got %q", n.TemplateID)
	}
}
