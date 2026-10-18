package model

import "time"

// Channel y SendStatus son enums cerrados (VARCHAR+CHECK en notification_db).
type Channel string
type SendStatus string

const (
	ChannelEmail Channel = "EMAIL"
	ChannelInApp Channel = "IN_APP"

	StatusPending SendStatus = "PENDING"
	StatusSent    SendStatus = "SENT"
	StatusFailed  SendStatus = "FAILED"
)

// SentNotification es la entidad de dominio (pura, sin tags de persistencia).
type SentNotification struct {
	ID             string
	RecipientID    string
	RecipientEmail string
	Channel        Channel
	Subject        string
	BodySummary    string // rendered body when a template was used; empty otherwise
	SendStatus     SendStatus
	FailureReason  string
	TemplateID     string // UUID of the template used; empty if no template was applied
	SourceService  string
	SourceEventID  string
	SentAt         *time.Time
	CreatedAt      time.Time
}
