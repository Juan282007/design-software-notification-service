package model

import "time"

// NotificationTemplate is the domain entity for a reusable notification template.
// SubjectTemplate and BodyTemplate support {{key}} placeholder substitution.
type NotificationTemplate struct {
	ID              string
	Code            string
	Channel         Channel
	SubjectTemplate string
	BodyTemplate    string
	IsActive        bool
	CreatedAt       time.Time
	UpdatedAt       time.Time
}
