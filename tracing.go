package pubsub

import "github.com/ThreeDotsLabs/watermill/message"

// messageCarrier adapts a Watermill message's metadata to
// propagation.TextMapCarrier so OpenTelemetry can inject/extract trace
// context across the wire.
type messageCarrier struct {
	msg *message.Message
}

func (c messageCarrier) Get(key string) string {
	return c.msg.Metadata.Get(key)
}

func (c messageCarrier) Set(key, value string) {
	c.msg.Metadata.Set(key, value)
}

func (c messageCarrier) Keys() []string {
	keys := make([]string, 0, len(c.msg.Metadata))
	for k := range c.msg.Metadata {
		keys = append(keys, k)
	}
	return keys
}
