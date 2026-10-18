package model

import "time"

// OutboxEvent is a domain event staged for at-least-once publishing (Outbox pattern,
// ADR-001). A relay adapter reads unpublished rows, publishes them to the broker, and
// stamps PublishedAt.
type OutboxEvent struct {
	ID          string
	EventID     string
	EventType   string
	Payload     []byte
	CreatedAt   time.Time
	PublishedAt *time.Time
}
