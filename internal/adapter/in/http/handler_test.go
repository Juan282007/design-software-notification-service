package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/code-sena/design-software-notification-service/internal/application/port/in"
	"github.com/code-sena/design-software-notification-service/internal/domain/model"
)

type fakeUseCase struct {
	called bool
	cmd    in.SendNotificationCommand
	result *model.SentNotification
	err    error
}

func (f *fakeUseCase) Handle(_ context.Context, cmd in.SendNotificationCommand) (*model.SentNotification, error) {
	f.called = true
	f.cmd = cmd
	if f.err != nil {
		return nil, f.err
	}
	return f.result, nil
}

type fakeGetUseCase struct {
	result *model.SentNotification
	err    error
}

func (f *fakeGetUseCase) Handle(_ context.Context, _ in.GetNotificationQuery) (*model.SentNotification, error) {
	return f.result, f.err
}

func doPost(h *Handler, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest("POST", "/notifications", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	h.Routes().ServeHTTP(rec, req)
	return rec
}

func doGet(h *Handler, id string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/notifications/"+id, nil)
	rec := httptest.NewRecorder()
	h.Routes().ServeHTTP(rec, req)
	return rec
}

// E1: payload valido -> 202 + delega en el use case.
func TestSend_Success_Returns202(t *testing.T) {
	uc := &fakeUseCase{result: &model.SentNotification{
		ID:          "11111111-1111-1111-1111-111111111111",
		RecipientID: "22222222-2222-2222-2222-222222222222",
		Channel:     model.ChannelEmail,
		Subject:     "hi",
		SendStatus:  model.StatusPending,
	}}
	h := NewHandler(uc, nil)

	rec := doPost(h, `{"recipient_id":"22222222-2222-2222-2222-222222222222","recipient_email":"a@b.co","channel":"EMAIL","subject":"hi"}`)

	if rec.Code != 202 {
		t.Fatalf("want 202, got %d: %s", rec.Code, rec.Body.String())
	}
	if !uc.called {
		t.Fatal("el use case no fue invocado")
	}
	if uc.cmd.RecipientEmail != "a@b.co" || uc.cmd.Channel != model.ChannelEmail {
		t.Fatalf("comando mal mapeado: %+v", uc.cmd)
	}
}

// E2: payload sin campos requeridos -> 400 VALIDATION_ERROR.
func TestSend_MissingRequiredFields_Returns400(t *testing.T) {
	uc := &fakeUseCase{}
	h := NewHandler(uc, nil)

	rec := doPost(h, `{"channel":"EMAIL"}`)

	if rec.Code != 400 {
		t.Fatalf("want 400, got %d: %s", rec.Code, rec.Body.String())
	}
	if uc.called {
		t.Fatal("el use case no debia invocarse con payload invalido")
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("respuesta no es JSON valido: %v", err)
	}
	if body["error_code"] != "VALIDATION_ERROR" {
		t.Fatalf("want error_code=VALIDATION_ERROR, got %q", body["error_code"])
	}
}

// E3: channel fuera de [EMAIL, IN_APP] -> 400 VALIDATION_ERROR.
func TestSend_InvalidChannel_Returns400(t *testing.T) {
	uc := &fakeUseCase{}
	h := NewHandler(uc, nil)

	rec := doPost(h, `{"recipient_id":"22222222-2222-2222-2222-222222222222","recipient_email":"a@b.co","channel":"SMS","subject":"hi"}`)

	if rec.Code != 400 {
		t.Fatalf("want 400, got %d: %s", rec.Code, rec.Body.String())
	}
	if uc.called {
		t.Fatal("el use case no debia invocarse con channel invalido")
	}
}

// error del use case (p.ej. repo caido) -> 503 DEPENDENCY_UNAVAILABLE, no 202.
func TestSend_UseCaseError_Returns503(t *testing.T) {
	uc := &fakeUseCase{err: errors.New("db down")}
	h := NewHandler(uc, nil)

	rec := doPost(h, `{"recipient_id":"22222222-2222-2222-2222-222222222222","recipient_email":"a@b.co","channel":"EMAIL","subject":"hi"}`)

	if rec.Code != 503 {
		t.Fatalf("want 503, got %d: %s", rec.Code, rec.Body.String())
	}
}

// HU-NOTIF-007: POST /notifications must emit an HTTP span (otelhttp instrumentation).
func TestSend_EmitsHTTPSpan(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))

	uc := &fakeUseCase{result: &model.SentNotification{
		ID:          "11111111-1111-1111-1111-111111111111",
		RecipientID: "22222222-2222-2222-2222-222222222222",
		Channel:     model.ChannelEmail,
		Subject:     "hi",
		SendStatus:  model.StatusPending,
	}}
	h := NewHandler(uc, WithTracerProvider(tp))

	rec := doPost(h, `{"recipient_id":"22222222-2222-2222-2222-222222222222","recipient_email":"a@b.co","channel":"EMAIL","subject":"hi"}`)
	if rec.Code != 202 {
		t.Fatalf("want 202, got %d: %s", rec.Code, rec.Body.String())
	}

	spans := exporter.GetSpans()
	if len(spans) == 0 {
		t.Fatal("expected at least one HTTP span to be emitted for POST /notifications")
	}
}

// HU-NOTIF-005 E1: GET /notifications/{id} existente -> 200 + SentNotification.
func TestGet_Found_Returns200(t *testing.T) {
	notif := &model.SentNotification{
		ID:          "11111111-1111-1111-1111-111111111111",
		RecipientID: "22222222-2222-2222-2222-222222222222",
		Channel:     model.ChannelEmail,
		Subject:     "hello",
		SendStatus:  model.StatusSent,
	}
	h := NewHandler(nil, WithGetUseCase(&fakeGetUseCase{result: notif}))

	rec := doGet(h, "11111111-1111-1111-1111-111111111111")

	if rec.Code != 200 {
		t.Fatalf("want 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("respuesta no es JSON valido: %v", err)
	}
	if body["send_status"] != "SENT" {
		t.Fatalf("want send_status=SENT, got %v", body["send_status"])
	}
}

// HU-NOTIF-005 E2: GET /notifications/{id} inexistente -> 404 NOT_FOUND.
func TestGet_NotFound_Returns404(t *testing.T) {
	h := NewHandler(nil, WithGetUseCase(&fakeGetUseCase{err: model.ErrNotFound}))

	rec := doGet(h, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

	if rec.Code != 404 {
		t.Fatalf("want 404, got %d: %s", rec.Code, rec.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("respuesta no es JSON valido: %v", err)
	}
	if body["error_code"] != "NOT_FOUND" {
		t.Fatalf("want error_code=NOT_FOUND, got %q", body["error_code"])
	}
}

// GET con id no-UUID -> 400 VALIDATION_ERROR.
func TestGet_InvalidUUID_Returns400(t *testing.T) {
	h := NewHandler(nil, WithGetUseCase(&fakeGetUseCase{}))

	rec := doGet(h, "not-a-uuid")

	if rec.Code != 400 {
		t.Fatalf("want 400, got %d: %s", rec.Code, rec.Body.String())
	}
}
