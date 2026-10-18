package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"syscall"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"

	amqpadapter "github.com/code-sena/design-software-notification-service/internal/adapter/in/amqp"
	"github.com/code-sena/design-software-notification-service/internal/adapter/out/client"
	"github.com/code-sena/design-software-notification-service/internal/adapter/out/messaging"
	"github.com/code-sena/design-software-notification-service/internal/adapter/out/notifier"
	"github.com/code-sena/design-software-notification-service/internal/adapter/out/persistence"
	"github.com/code-sena/design-software-notification-service/internal/application/usecase"
	"github.com/code-sena/design-software-notification-service/internal/platform/logging"
	otelplatform "github.com/code-sena/design-software-notification-service/internal/platform/otel"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger := logging.New()

	dbDSN := requireEnv("NOTIFICATION_DB_DSN")
	amqpURL := requireEnv("NOTIFICATION_AMQP_URL")
	smtpAddr := requireEnv("NOTIFICATION_SMTP_ADDR")
	smtpFrom := envOr("NOTIFICATION_SMTP_FROM", "notifications@sena.local")
	stubEmail := envOr("NOTIFICATION_RECIPIENT_STUB_EMAIL", "dev-notifications@sena.local")

	providers, err := otelplatform.Setup(ctx, otelplatform.Config{
		ServiceName:  "notification-worker",
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

	pool, err := persistence.NewPoolWithTracing(ctx, dbDSN, providers.TracerProvider)
	if err != nil {
		log.Fatalf("failed to open notification_db pool: %v", err)
	}
	defer pool.Close()

	metrics, err := otelplatform.NewMetrics(providers.MeterProvider.Meter("notification-worker"))
	if err != nil {
		log.Fatalf("failed to create metrics instruments: %v", err)
	}

	repo := persistence.NewPgNotificationRepository(pool)
	tmplRepo := persistence.NewPgTemplateRepository(pool)
	resolver := client.NewStubRecipientResolver(stubEmail)
	notify := notifier.NewCompositeNotifier(
		notifier.NewSMTPNotifier(smtpAddr, smtpFrom),
		notifier.NewInAppNotifier(),
		metrics,
	)
	uc := usecase.NewConsumeDomainEvent(resolver, notify, repo, tmplRepo)

	retryCfg := amqpadapter.RetryConfig{
		MaxAttempts:     envIntOr("NOTIFICATION_RETRY_MAX_ATTEMPTS", 3),
		InitialInterval: time.Duration(envIntOr("NOTIFICATION_RETRY_INITIAL_INTERVAL_MS", 500)) * time.Millisecond,
		MaxInterval:     time.Duration(envIntOr("NOTIFICATION_RETRY_MAX_INTERVAL_MS", 30000)) * time.Millisecond,
	}

	consumer, err := amqpadapter.NewConsumer(amqpURL, uc, metrics, retryCfg)
	if err != nil {
		log.Fatalf("failed to start AMQP consumer: %v", err)
	}
	defer consumer.Close()

	relayConn, err := amqp091.Dial(amqpURL)
	if err != nil {
		log.Fatalf("failed to open AMQP connection for the outbox relay: %v", err)
	}
	defer relayConn.Close()
	relayCh, err := relayConn.Channel()
	if err != nil {
		log.Fatalf("failed to open AMQP channel for the outbox relay: %v", err)
	}
	defer relayCh.Close()

	relay, err := messaging.NewOutboxRelay(pool, relayCh)
	if err != nil {
		log.Fatalf("failed to start outbox relay: %v", err)
	}
	go relay.Run(ctx, 2*time.Second, 20, func(err error) {
		logging.FromContext(ctx, logger).Error("outbox relay error", "error", err.Error())
	})

	go serveHealth(ctx, envOr("WORKER_HEALTH_PORT", "8081"), repo, consumer)

	log.Println("notification-worker: consuming scheduling.schedule.published, monitoring.alert.triggered")
	if err := consumer.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatalf("consumer stopped: %v", err)
	}
	log.Println("notification-worker: shutting down")
}

// pinger mirrors adapter/in/http.Pinger without importing that package (worker has no
// business reason to depend on the HTTP adapter).
type pinger interface {
	Ping(ctx context.Context) error
}

// serveHealth exposes GET /health and GET /ready for the worker container -- unlike the
// API, the worker has no other HTTP surface, so this is a small dedicated server rather
// than reusing adapter/in/http.Handler (which is coupled to the send-notification route).
func serveHealth(ctx context.Context, port string, checks ...pinger) {
	named := map[string]pinger{"database": checks[0], "broker": checks[1]}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, r *http.Request) {
		type result struct {
			Name  string `json:"name"`
			OK    bool   `json:"ok"`
			Error string `json:"error,omitempty"`
		}
		var results []result
		allOK := true
		for name, p := range named {
			res := result{Name: name, OK: true}
			if err := p.Ping(r.Context()); err != nil {
				res.OK, res.Error, allOK = false, err.Error(), false
			}
			results = append(results, res)
		}
		sort.Slice(results, func(i, j int) bool { return results[i].Name < results[j].Name })
		status, code := "ok", http.StatusOK
		if !allOK {
			status, code = "degraded", http.StatusServiceUnavailable
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": status, "checks": results})
	})

	srv := &http.Server{Addr: ":" + port, Handler: mux}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	log.Printf("notification-worker: health/ready listening on :%s", port)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("notification-worker: health server error: %v", err)
	}
}

func requireEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		log.Fatalf("%s is required; no default is provided for connection secrets", k)
	}
	return v
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envIntOr(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
