package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"regexp"
	"time"

	"github.com/google/uuid"
)

var emailRegexp = regexp.MustCompile(`^[^@\s]+@[^@\s]+\.[^@\s]+$`)

// EmailSendPayload is the request shape for the email_send job type.
type EmailSendPayload struct {
	Recipient string `json:"recipient"`
	Subject   string `json:"subject"`
	Body      string `json:"body"`
}

type EmailSendResult struct {
	Simulated bool   `json:"simulated"`
	MessageID string `json:"message_id"`
	Recipient string `json:"recipient"`
}

// EmailSendHandler simulates sending an email over SMTP: it validates the
// payload shape, sleeps a jittered duration to stand in for network
// latency, and returns a fake message ID. No real email is ever sent — a
// portfolio demo has no business spamming real inboxes with test traffic,
// and no SMTP credentials are provisioned. This is documented in the
// README's Design Decisions section.
type EmailSendHandler struct{}

func (h *EmailSendHandler) Execute(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	var p EmailSendPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("invalid email_send payload: %w", err)
	}
	if !emailRegexp.MatchString(p.Recipient) {
		return nil, fmt.Errorf("invalid recipient email address")
	}
	if p.Subject == "" {
		return nil, fmt.Errorf("subject must not be empty")
	}
	if p.Body == "" {
		return nil, fmt.Errorf("body must not be empty")
	}

	jitterMs := 200 + rand.Intn(600) // 200-800ms
	select {
	case <-time.After(time.Duration(jitterMs) * time.Millisecond):
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	out := EmailSendResult{
		Simulated: true,
		MessageID: uuid.New().String(),
		Recipient: p.Recipient,
	}
	return json.Marshal(out)
}
