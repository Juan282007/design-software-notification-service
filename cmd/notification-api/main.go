package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	httpadapter "github.com/code-sena/design-software-notification-service/internal/adapter/in/http"
	"github.com/code-sena/design-software-notification-service/internal/adapter/out/notifier"
	"github.com/code-sena/design-software-notification-service/internal/adapter/out/persistence"
	"github.com/code-sena/design-software-notification-service/internal/application/usecase"
	"github.com/code-sena/design-software-notification-service/internal/platform/logging"
	otelplatform "github.com/code-sena/design-software-notification-service/internal/platform/otel"
)

func main() {
	ctx := context.Background()
	logger := logging.New()

	dsn := os.Getenv("NOTIFICATION_DB_DSN")
	if dsn == "" {
		log.Fatal("NOTIFICATION_DB_DSN is required (e.g. postgres://<user>:<password>@<host>:<port>/<db>?sslmode=disable); no default is provided for secrets")
	}

	providers, err := otelplatform.Setup(ctx, otelplatform.Config{
		ServiceName:  "notification-api",
		Environment:  envOr("NOTIFICATION_DEPLOYMENT_ENVIRONMENT", "develop"),
		OTLPEndpoint: envOr("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317"),
		Insecure:     envOr("OTEL_EXPORTER_OTLP_INSECURE", "true") == "true",
	})
	if err != nil {
		log.Fatalf("failed to set up OpenTelemetry: %v", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := providers.Shutdown(shutdownCtx); err != nil {
			logger.Error("otel shutdown error", "error", err.Error())
		}
	}()

	pool, err := persistence.NewPoolWithTracing(ctx, dsn, providers.TracerProvider)
	if err != nil {
		log.Fatalf("failed to open notification_db pool: %v", err)
	}
	defer pool.Close()

	metrics, err := otelplatform.NewMetrics(providers.MeterProvider.Meter("notification-api"))
	if err != nil {
		log.Fatalf("failed to create metrics instruments: %v", err)
	}

	// composition root: wiring adapters -> application -> domain.
	repo := persistence.NewPgNotificationRepository(pool)
	tmplRepo := persistence.NewPgTemplateRepository(pool)
	smtpAddr := envOr("NOTIFICATION_SMTP_ADDR", "mailhog:1025")
	smtpFrom := envOr("NOTIFICATION_SMTP_FROM", "notifications@sena.local")
	composite := notifier.NewCompositeNotifier(
		notifier.NewSMTPNotifier(smtpAddr, smtpFrom),
		notifier.NewInAppNotifier(),
		metrics,
	)
	sendUC := usecase.NewSendNotification(repo, tmplRepo, composite)
	getUC := usecase.NewGetNotification(repo)
	h := httpadapter.NewHandler(sendUC,
		httpadapter.WithGetUseCase(getUC),
		httpadapter.WithMetrics(metrics),
		httpadapter.WithLogger(logger),
		httpadapter.WithReadinessChecks(map[string]httpadapter.Pinger{"database": repo}),
	)

	addr := ":" + envOr("PORT", "8080")
	log.Printf("notification-api listening on %s", addr)
	if err := http.ListenAndServe(addr, h.Routes()); err != nil {
		log.Fatal(err)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
