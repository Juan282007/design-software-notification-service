package in

import "context"

// ConsumeDomainEventCommand carries the fields the worker needs from a decoded
// DomainEventEnvelope (shared-contracts/events/event-envelope.schema.json).
//
// TraceParent is an opaque W3C traceparent string (see
// https://www.w3.org/TR/trace-context/) for the span that received this event. It is
// plain data here -- the adapter that filled it in already did the OTel work; the use
// case only carries it through into the outbox payload so a later, decoupled publish
// (OutboxRelay) can continue the same trace. No OTel package is imported by this
// package or by the use case that consumes it.
type ConsumeDomainEventCommand struct {
	EventID       string
	EventType     string
	SourceService string
	Payload       map[string]interface{}
	TraceParent   string
}

// ConsumeDomainEventUseCase: puerto de entrada del worker (HU-NOTIF-002).
type ConsumeDomainEventUseCase interface {
	Handle(ctx context.Context, cmd ConsumeDomainEventCommand) error
}
