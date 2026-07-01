// Package pubsub provides a small, transport-agnostic publish/subscribe
// abstraction on top of Watermill, with built-in OpenTelemetry tracing.
//
// A Bus is constructed with New, selecting exactly one transport via a
// driver Option (WithGoChannel, WithRedisStream, WithKafka, WithPostgres,
// WithRabbitMQ) plus any number of tuning options:
//
//	bus, err := pubsub.New(
//		pubsub.WithRedisStream(redisClient),
//		pubsub.WithConsumerGroup("orders-service"),
//		pubsub.WithLogger(logger),
//	)
package pubsub

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ThreeDotsLabs/watermill/message"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/ThreeDotsLabs/watermill"
)

const tracerName = "pubsub"

// Handler processes a single message payload delivered on a topic.
// Returning an error causes the message to be nacked and redelivered
// according to the underlying transport's semantics.
type Handler func(context.Context, []byte) error

// Bus is a transport-agnostic publish/subscribe interface.
type Bus interface {
	// Publish sends payload to topic.
	Publish(ctx context.Context, topic string, payload []byte) error

	// Subscribe blocks, delivering messages on topic to handler until ctx
	// is cancelled or the subscription ends.
	Subscribe(ctx context.Context, topic string, handler Handler) error

	// Close releases all underlying transport resources. It is safe to
	// call multiple times.
	Close() error
}

// closer is satisfied by every Watermill publisher/subscriber.
type closer interface{ Close() error }

type watermillBus struct {
	publisher  message.Publisher
	subscriber message.Subscriber
	closers    []closer

	tracer     trace.Tracer
	propagator propagation.TextMapPropagator

	once     sync.Once
	closeErr error
}

func (b *watermillBus) Publish(ctx context.Context, topic string, payload []byte) error {
	if b == nil || b.publisher == nil {
		return errors.New("pubsub: bus is not configured")
	}

	ctx, span := b.tracer.Start(ctx, fmt.Sprintf("%s publish", topic),
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.MessagingSystemKey.String("watermill"),
			semconv.MessagingDestinationName(topic),
			semconv.MessagingOperationTypePublish,
			attribute.Int("messaging.message.body.size", len(payload)),
		),
	)
	defer span.End()

	msg := message.NewMessage(watermill.NewUUID(), payload)
	msg.SetContext(ctx)

	// Inject trace context into message metadata for propagation.
	b.propagator.Inject(ctx, messageCarrier{msg: msg})

	span.SetAttributes(attribute.String("messaging.message.id", msg.UUID))

	if err := b.publisher.Publish(topic, msg); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	return nil
}

func (b *watermillBus) Subscribe(ctx context.Context, topic string, handler Handler) error {
	if b == nil || b.subscriber == nil {
		return errors.New("pubsub: bus is not configured")
	}

	messages, err := b.subscriber.Subscribe(ctx, topic)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-messages:
			if !ok {
				return nil
			}
			if handler == nil {
				msg.Ack()
				continue
			}
			if err := b.handleMessage(ctx, topic, msg, handler); err != nil {
				return fmt.Errorf("pubsub: handle message: %w", err)
			}
		}
	}
}

func (b *watermillBus) handleMessage(ctx context.Context, topic string, msg *message.Message, handler Handler) error {
	// Extract trace context from message metadata.
	msgCtx := b.propagator.Extract(msg.Context(), messageCarrier{msg: msg})

	msgCtx, span := b.tracer.Start(msgCtx, fmt.Sprintf("%s process", topic),
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			semconv.MessagingSystemKey.String("watermill"),
			semconv.MessagingDestinationName(topic),
			semconv.MessagingOperationTypeReceive,
			attribute.String("messaging.message.id", msg.UUID),
			attribute.Int("messaging.message.body.size", len(msg.Payload)),
		),
	)
	defer span.End()

	if err := handler(msgCtx, msg.Payload); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		msg.Nack()
		return err
	}

	span.SetAttributes(semconv.MessagingOperationTypeSettle)
	span.SetStatus(codes.Ok, "message processed successfully")
	msg.Ack()
	return nil
}

func (b *watermillBus) Close() error {
	if b == nil {
		return nil
	}

	b.once.Do(func() {
		for _, c := range b.closers {
			if c == nil {
				continue
			}
			if err := c.Close(); err != nil {
				b.closeErr = errors.Join(b.closeErr, err)
			}
		}
	})

	return b.closeErr
}
