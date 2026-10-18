package http

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sort"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/code-sena/design-software-notification-service/api"
	"github.com/code-sena/design-software-notification-service/internal/application/port/in"
	"github.com/code-sena/design-software-notification-service/internal/domain/model"
	"github.com/code-sena/design-software-notification-service/internal/platform/logging"
	otelplatform "github.com/code-sena/design-software-notification-service/internal/platform/otel"
)

// Pinger is a minimal dependency-health check used by /ready (e.g. a DB pool or an AMQP
// connection). Kept local to this adapter so it doesn't depend on concrete infra types.
type Pinger interface {
	Ping(ctx context.Context) error
}

type Handler struct {
	uc             in.SendNotificationUseCase
	getUC          in.GetNotificationUseCase
	metrics        *otelplatform.Metrics
	checks         map[string]Pinger
	logger         *slog.Logger
	tracerProvider trace.TracerProvider // optional; nil lets otelhttp use the global one
}

type Option func(*Handler)

func WithMetrics(m *otelplatform.Metrics) Option { return func(h *Handler) { h.metrics = m } }

func WithReadinessChecks(checks map[string]Pinger) Option {
	return func(h *Handler) { h.checks = checks }
}

func WithLogger(l *slog.Logger) Option { return func(h *Handler) { h.logger = l } }

// WithTracerProvider pins otelhttp to an explicit TracerProvider instead of the
// process-global one. Mainly for tests: otel's global delegate can only really be
// (re)assigned once per process (see the equivalent comment in
// adapter/in/amqp/consumer.go), so tests that need their own exporter inject it here.
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(h *Handler) { h.tracerProvider = tp }
}

func WithGetUseCase(uc in.GetNotificationUseCase) Option {
	return func(h *Handler) { h.getUC = uc }
}

func NewHandler(uc in.SendNotificationUseCase, opts ...Option) *Handler {
	h := &Handler{uc: uc, logger: logging.New()}
	for _, opt := range opts {
		if opt != nil {
			opt(h)
		}
	}
	return h
}

// Routes returns the traced (otelhttp) and, when WithMetrics was set, RED-metered HTTP
// handler for this service. Returning http.Handler (not *http.ServeMux) keeps the
// wrapping transparent to callers (cmd/notification-api, tests).
func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", h.health)
	mux.HandleFunc("GET /ready", h.ready)
	mux.HandleFunc("POST /notifications", h.send)
	mux.HandleFunc("GET /notifications/{id}", h.get)

	var otelOpts []otelhttp.Option
	if h.tracerProvider != nil {
		otelOpts = append(otelOpts, otelhttp.WithTracerProvider(h.tracerProvider))
	}
	traced := otelhttp.NewHandler(mux, "notification-api", otelOpts...)
	if h.metrics != nil {
		return h.withREDMetrics(traced)
	}
	return traced
}

func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// ready checks dependencies registered via WithReadinessChecks (DB, broker, ...) and
// reports 200 only if all of them respond.
func (h *Handler) ready(w http.ResponseWriter, r *http.Request) {
	type checkResult struct {
		Name  string `json:"name"`
		OK    bool   `json:"ok"`
		Error string `json:"error,omitempty"`
	}
	results := make([]checkResult, 0, len(h.checks))
	allOK := true
	for name, pinger := range h.checks {
		cr := checkResult{Name: name, OK: true}
		if err := pinger.Ping(r.Context()); err != nil {
			cr.OK = false
			cr.Error = err.Error()
			allOK = false
		}
		results = append(results, cr)
	}
	sort.Slice(results, func(i, j int) bool { return results[i].Name < results[j].Name })

	status := "ok"
	code := http.StatusOK
	if !allOK {
		status = "degraded"
		code = http.StatusServiceUnavailable
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{"status": status, "checks": results})
}

// errorEnvelope sigue el shape transversal de shared-contracts/errors/error-codes.yaml.
type errorEnvelope struct {
	ErrorCode string `json:"error_code"`
	Message   string `json:"message"`
	TraceID   string `json:"trace_id,omitempty"`
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorEnvelope{ErrorCode: code, Message: msg})
}

// send implementa POST /notifications (api.SendNotificationRequest, generado desde
// shared-contracts/openapi/notification.yaml). Valida el contrato, delega en el use case
// y responde 202 con el api.SentNotification generado.
func (h *Handler) send(w http.ResponseWriter, r *http.Request) {
	var req api.SendNotificationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "payload invalido: "+err.Error())
		return
	}
	if err := validateSendRequest(req); err != nil {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}

	cmd := in.SendNotificationCommand{
		RecipientID:    req.RecipientId.String(),
		RecipientEmail: string(req.RecipientEmail),
		Channel:        model.Channel(req.Channel),
		Subject:        req.Subject,
	}
	if req.TemplateCode != nil {
		cmd.TemplateCode = *req.TemplateCode
	}
	if req.TemplateVars != nil {
		cmd.TemplateVars = *req.TemplateVars
	}
	if req.SourceService != nil {
		cmd.SourceService = *req.SourceService
	}
	if req.SourceEventId != nil {
		cmd.SourceEventID = req.SourceEventId.String()
	}

	n, err := h.uc.Handle(r.Context(), cmd)
	if err != nil {
		logging.FromContext(r.Context(), h.logger).Error("failed to persist notification", "error", err.Error())
		writeError(w, http.StatusServiceUnavailable, "DEPENDENCY_UNAVAILABLE", "no se pudo persistir la notificacion")
		return
	}

	resp := toSentNotification(n)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(resp)
}

// get implementa GET /notifications/{id} (HU-NOTIF-005).
// E1: devuelve 200 + SentNotification cuando el id existe.
// E2: devuelve 404 NOT_FOUND cuando no existe.
func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "id must be a valid UUID")
		return
	}

	n, err := h.getUC.Handle(r.Context(), in.GetNotificationQuery{ID: id})
	if err != nil {
		if errors.Is(err, model.ErrNotFound) {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "notification not found")
			return
		}
		writeError(w, http.StatusServiceUnavailable, "DEPENDENCY_UNAVAILABLE", "error retrieving notification")
		return
	}

	resp := toSentNotification(n)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

var errMissingFields = errors.New("recipient_id, recipient_email y subject son requeridos")
var errInvalidChannel = errors.New("channel debe ser EMAIL o IN_APP")

// validateSendRequest cubre lo que el tipado generado no fuerza en Go (campos "required"
// del schema no bloquean un JSON que los omite; ahi el zero-value pasa desapercibido).
func validateSendRequest(req api.SendNotificationRequest) error {
	if req.RecipientId == uuid.Nil || req.RecipientEmail == "" || req.Subject == "" {
		return errMissingFields
	}
	if !req.Channel.Valid() {
		return errInvalidChannel
	}
	return nil
}

func toSentNotification(n *model.SentNotification) api.SentNotification {
	id, _ := uuid.Parse(n.ID)
	recipientID, _ := uuid.Parse(n.RecipientID)
	channel := api.SentNotificationChannel(n.Channel)
	status := api.SentNotificationSendStatus(n.SendStatus)
	return api.SentNotification{
		Id:          &id,
		RecipientId: &recipientID,
		Channel:     &channel,
		SendStatus:  &status,
		Subject:     &n.Subject,
		SentAt:      n.SentAt,
	}
}

// withREDMetrics records request count + duration (RED: rate/errors/duration -- "errors"
// is derivable from the status_class attribute) for every request through next.
func (h *Handler) withREDMetrics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		duration := time.Since(start).Seconds()

		attrs := metric.WithAttributes(
			attribute.String("http.method", r.Method),
			attribute.String("http.route", r.URL.Path),
			attribute.Int("http.status_code", sw.status),
		)
		h.metrics.HTTPRequestsTotal.Add(r.Context(), 1, attrs)
		h.metrics.HTTPRequestDuration.Record(r.Context(), duration, attrs)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}
