package notifier

import (
	"context"
	"fmt"
	"net/smtp"

	"github.com/code-sena/design-software-notification-service/internal/domain/model"
)

// SMTPNotifier delivers EMAIL notifications through an SMTP relay (MailHog in local/dev,
// mailhog:1025 from inside docker-infra's network; no auth required).
type SMTPNotifier struct {
	addr string // host:port
	from string
}

func NewSMTPNotifier(addr, from string) *SMTPNotifier {
	return &SMTPNotifier{addr: addr, from: from}
}

func (s *SMTPNotifier) Send(_ context.Context, n *model.SentNotification) error {
	body := n.BodySummary
	if body == "" {
		body = n.Subject
	}
	msg := fmt.Sprintf("From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n%s\r\n",
		s.from, n.RecipientEmail, n.Subject, body)
	return smtp.SendMail(s.addr, nil, s.from, []string{n.RecipientEmail}, []byte(msg))
}
