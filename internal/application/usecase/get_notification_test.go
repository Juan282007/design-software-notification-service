package usecase

import (
	"context"
	"errors"
	"testing"

	"github.com/code-sena/design-software-notification-service/internal/application/port/in"
	"github.com/code-sena/design-software-notification-service/internal/domain/model"
)

// E1: repo encuentra la notificacion -> use case la devuelve.
func TestGetNotification_Found(t *testing.T) {
	want := &model.SentNotification{
		ID:         "11111111-1111-1111-1111-111111111111",
		Channel:    model.ChannelEmail,
		Subject:    "subject",
		SendStatus: model.StatusSent,
	}
	repo := &fakeRepo{foundByID: want}
	uc := NewGetNotification(repo)

	got, err := uc.Handle(context.Background(), in.GetNotificationQuery{ID: want.ID})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.ID != want.ID {
		t.Fatalf("want ID=%s, got %s", want.ID, got.ID)
	}
}

// E2: repo no encuentra fila (nil, nil) -> use case retorna ErrNotFound.
func TestGetNotification_NotFound(t *testing.T) {
	repo := &fakeRepo{foundByID: nil, findErr: nil}
	uc := NewGetNotification(repo)

	_, err := uc.Handle(context.Background(), in.GetNotificationQuery{ID: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"})
	if !errors.Is(err, model.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

// error de infraestructura del repo -> se propaga sin envolver.
func TestGetNotification_PropagatesRepoError(t *testing.T) {
	repo := &fakeRepo{findErr: errors.New("db down")}
	uc := NewGetNotification(repo)

	_, err := uc.Handle(context.Background(), in.GetNotificationQuery{ID: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"})
	if err == nil {
		t.Fatal("expected repo error, got nil")
	}
}
