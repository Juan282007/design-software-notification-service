package otel

import amqp091 "github.com/rabbitmq/amqp091-go"

// AMQPHeaderCarrier adapts amqp091.Table to propagation.TextMapCarrier so W3C trace
// context (traceparent/tracestate) can be injected into, or extracted from, AMQP
// message headers -- this is how a trace crosses the async hop between publish and
// consume (ADR-008 §4).
type AMQPHeaderCarrier amqp091.Table

func (c AMQPHeaderCarrier) Get(key string) string {
	v, ok := c[key]
	if !ok {
		return ""
	}
	s, _ := v.(string)
	return s
}

func (c AMQPHeaderCarrier) Set(key, value string) {
	c[key] = value
}

func (c AMQPHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}
