# design-software-notification-service (Go)

Application layer of **notification-service** (transversal delivery capability). ADR-005/006.
**Hexagonal** architecture (DDD) · contract-first (SDD, `shared-contracts`) · TDD.

## Structure
```
internal/domain        pure entities (SentNotification, OutboxEvent, Recipient) — no infra
internal/application   usecase + port/in + port/out
internal/adapter        in/http · in/amqp · out/persistence (→ notification_db)
                         out/messaging (outbox relay) · out/notifier (SMTP/in-app) · out/client
cmd/notification-api    HTTP container
cmd/notification-worker AMQP consumer + outbox relay container
```
Rule: `cmd → adapter → application → domain`.

## Running
```
export NOTIFICATION_DB_DSN="postgres://<user>:<password>@<host>:<port>/<db>?sslmode=disable"
go run ./cmd/notification-api      # :8080  (GET /health, POST /notifications)

export NOTIFICATION_AMQP_URL="amqp://<user>:<password>@<host>:<port>/"
export NOTIFICATION_SMTP_ADDR="<host>:<port>"      # MailHog SMTP (mailhog:1025 in docker-infra)
go run ./cmd/notification-worker   # consumes scheduling.schedule.published, monitoring.alert.triggered

go test ./...                      # unit
go test -tags=integration ./...    # + integration (needs the env vars above; skips per-test if unset)
```
DB: schema `notification` (repo `design-software-notification-db`, live in all 4 environments).
No credentials or DSN/AMQP defaults exist in the code — `NOTIFICATION_DB_DSN`,
`NOTIFICATION_AMQP_URL` and `NOTIFICATION_SMTP_ADDR` are all required (`log.Fatal` if unset).
For local `develop`, use the least-privilege user `design_software_app` against `localhost:15432`
(see skill `design-software-project`).

## Contract-first (SDD)
`api/` holds DTOs generated from `shared-contracts` — never hand-edited (see `api/doc.go` for the
exact regeneration commands and a note on a JSON Schema bug found upstream in
`event-envelope.schema.json`):
- `notification.gen.go` ← `openapi/notification.yaml` (`oapi-codegen`).
- `event_envelope.gen.go` ← `events/event-envelope.schema.json` (`go-jsonschema`).

## HU-NOTIF-001 (implemented)
`POST /notifications` validates the contract (required fields + `channel` in `[EMAIL, IN_APP]`),
delegates to the `SendNotification` use case, and persists to `notification.sent_notification` via
pgx (`send_status` starts `PENDING`; `id`/`created_at` come from the table).

## HU-NOTIF-002 (implemented)
`notification-worker` consumes `scheduling.schedule.published` and `monitoring.alert.triggered`
(RabbitMQ, topic exchanges `scheduling-events`/`monitoring-events` per ADR-001) and, per event:
resolves a recipient (`RecipientResolver`; today a configurable stub — see the TODO in
`internal/application/port/out/recipient_resolver.go`, the real lookup needs actors-service, which
doesn't exist yet), attempts delivery (`Notifier`: SMTP to MailHog for EMAIL, no-op for IN_APP), and
persists the resulting `SentNotification` (`SENT`/`FAILED`). Idempotency (no double delivery for a
redelivered `event_id`) is enforced at the database level via a unique index on
`sent_notification.source_event_id`, not just app logic. On a successful send, it also stages
`notification.notification.sent` in `notification.outbox` in the **same transaction** as the status
change; a separate `OutboxRelay` polls unpublished rows and publishes them to `notification-events`
(consumed by `audit-service`), marking `published_at`.

Deliberate simplification: `scheduling.schedule.published` carries `instructor_ids` (plural), not a
single recipient; this HU notifies `published_by` only (a "your schedule was published"
confirmation) rather than fanning out one notification per instructor, since fan-out would conflict
with the one-row-per-`source_event_id` idempotency index. See `recipientRefFor` in
`internal/application/usecase/consume_domain_event.go`.

Both blockers noted when this HU was built (missing `notification.outbox` migration, MailHog's SMTP
port not reachable from the host) have since been resolved; all integration tests are green.

## HU-NOTIF-007 (implemented)
OpenTelemetry instrumentation (SDK bootstrap lives in `internal/platform/otel`, wired only from the
composition roots `cmd/*/main.go` — never imported by `internal/domain` or `internal/application`).

- **Traces**: HTTP server via `otelhttp` (`adapter/in/http`); AMQP consumer/publisher instrumented
  by hand (no standard library for amqp091-go) with manual spans in `adapter/in/amqp` and
  `adapter/out/messaging`; pgx queries via `otelpgx` (`persistence.NewPoolWithTracing`).
  **Context propagation across the async hop**: the AMQP consumer extracts an upstream
  `traceparent` from message headers if present, and hands the use case a `TraceParent` string
  (plain data, no OTel import in `application`) that gets embedded in the outbox payload; when
  `OutboxRelay` later publishes `notification.notification.sent`, it extracts that `trace_parent`
  as the parent span context (continuing the *same* trace across the consume→outbox→publish gap)
  and injects its own span into the outgoing AMQP headers for downstream consumers.
- **Metrics**: RED (`http.server.requests`, `http.server.request.duration`) in `adapter/in/http`,
  plus `notification.delivered{channel,status}` recorded in `adapter/out/notifier.CompositeNotifier`
  (the one place that knows both the channel and the real delivery outcome).
- **Logs**: structured JSON via `internal/platform/logging` (`log/slog`), with `trace_id`/`span_id`
  embedded from the active span when logging errors.
- **Export**: OTLP/gRPC to the Collector (`OTEL_EXPORTER_OTLP_ENDPOINT`, default `localhost:4317` —
  not a secret, so a dev default is fine unlike the DB/AMQP DSNs). Resource carries
  `service.name` + `deployment.environment` (`NOTIFICATION_DEPLOYMENT_ENVIRONMENT`, default `develop`).
- **Health/readiness**: `GET /health` (liveness) and `GET /ready` (checks registered `Pinger`s —
  DB for the API, DB+broker for the worker's own small health server on `WORKER_HEALTH_PORT`,
  default `8081`, since the worker has no other HTTP surface).

Gotcha worth knowing if you add more OTel-touching tests: `otel.SetTracerProvider` /
`otel.SetTextMapPropagator` can only really be (re)assigned **once per process** (`sync.Once` in
`go.opentelemetry.io/otel/internal/global`) — swapping the global provider between tests silently
stops working after the first test that does it. Adapters here inject their tracer/propagator as an
explicit field (`Consumer.tracer`, `OutboxRelay.tracer`, `Handler.tracerProvider` via
`WithTracerProvider`) defaulting to the global one in production, so tests can hand in their own
`tracetest.InMemoryExporter`-backed provider without touching global state at all.

Pending (out of this HU): real channel delivery hardening (HU-NOTIF-003), retries/backoff/DLQ
(HU-NOTIF-004), templates (HU-NOTIF-006), Pact contract client.
