package pubsub

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/ThreeDotsLabs/watermill"
	amqp "github.com/ThreeDotsLabs/watermill-amqp/v3/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	sqlpub "github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// builder constructs the underlying publisher/subscriber pair for a
// transport, given the fully-resolved Config (logger, consumer group, etc.
// have already been applied by the time it runs).
type builder func(cfg *config) (message.Publisher, message.Subscriber, []closer, error)

// config accumulates everything New needs to build a Bus. It is unexported;
// callers only ever touch it through Option functions.
type config struct {
	logger        watermill.LoggerAdapter
	tracer        trace.Tracer
	propagator    propagation.TextMapPropagator
	consumerGroup string

	build builder
}

// Option configures a Bus. Exactly one driver Option (WithGoChannel,
// WithRedisStream, WithKafka, WithPostgres, WithRabbitMQ) must be supplied
// to New; the rest are optional tuning knobs and may be given in any order.
type Option func(*config)

// --- Driver options -------------------------------------------------------

// WithGoChannel selects an in-memory transport, suitable for tests and
// single-process deployments.
func WithGoChannel() Option {
	return func(c *config) {
		c.build = func(cfg *config) (message.Publisher, message.Subscriber, []closer, error) {
			channel := gochannel.NewGoChannel(gochannel.Config{}, cfg.logger)
			return channel, channel, []closer{channel}, nil
		}
	}
}

// WithRedisStream selects Redis Streams as the transport.
func WithRedisStream(client redis.UniversalClient) Option {
	return func(c *config) {
		c.build = func(cfg *config) (message.Publisher, message.Subscriber, []closer, error) {
			if client == nil {
				return nil, nil, nil, errors.New("pubsub: redis client is required")
			}

			publisher, err := redisstream.NewPublisher(
				redisstream.PublisherConfig{Client: client},
				cfg.logger,
			)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("pubsub: create redisstream publisher: %w", err)
			}

			subscriberConfig := redisstream.SubscriberConfig{
				Client:         client,
				FanOutOldestId: "$",
				OldestId:       "$",
			}
			if cfg.consumerGroup != "" {
				subscriberConfig.ConsumerGroup = cfg.consumerGroup
			}
			subscriber, err := redisstream.NewSubscriber(subscriberConfig, cfg.logger)
			if err != nil {
				_ = publisher.Close()
				return nil, nil, nil, fmt.Errorf("pubsub: create redisstream subscriber: %w", err)
			}

			return publisher, subscriber, []closer{publisher, subscriber}, nil
		}
	}
}

// WithKafka selects Kafka as the transport.
func WithKafka(brokers []string) Option {
	return func(c *config) {
		c.build = func(cfg *config) (message.Publisher, message.Subscriber, []closer, error) {
			if len(brokers) == 0 {
				return nil, nil, nil, errors.New("pubsub: missing kafka brokers")
			}

			publisher, err := kafka.NewPublisher(
				kafka.PublisherConfig{Brokers: brokers},
				cfg.logger,
			)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("pubsub: create kafka publisher: %w", err)
			}

			subscriber, err := kafka.NewSubscriber(
				kafka.SubscriberConfig{
					Brokers:       brokers,
					ConsumerGroup: cfg.consumerGroup,
				},
				cfg.logger,
			)
			if err != nil {
				_ = publisher.Close()
				return nil, nil, nil, fmt.Errorf("pubsub: create kafka subscriber: %w", err)
			}

			return publisher, subscriber, []closer{publisher, subscriber}, nil
		}
	}
}

// WithPostgres selects Postgres (via LISTEN/NOTIFY-backed polling) as the
// transport.
func WithPostgres(db *sql.DB) Option {
	return func(c *config) {
		c.build = func(cfg *config) (message.Publisher, message.Subscriber, []closer, error) {
			if db == nil {
				return nil, nil, nil, errors.New("pubsub: postgres database is required")
			}

			publisher, err := sqlpub.NewPublisher(
				sqlpub.BeginnerFromStdSQL(db),
				sqlpub.PublisherConfig{
					SchemaAdapter: sqlpub.DefaultPostgreSQLSchema{},
				},
				cfg.logger,
			)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("pubsub: create postgres publisher: %w", err)
			}

			subscriber, err := sqlpub.NewSubscriber(
				sqlpub.BeginnerFromStdSQL(db),
				sqlpub.SubscriberConfig{
					ConsumerGroup:    cfg.consumerGroup,
					SchemaAdapter:    sqlpub.DefaultPostgreSQLSchema{},
					OffsetsAdapter:   sqlpub.DefaultPostgreSQLOffsetsAdapter{},
					InitializeSchema: true,
				},
				cfg.logger,
			)
			if err != nil {
				_ = publisher.Close()
				return nil, nil, nil, fmt.Errorf("pubsub: create postgres subscriber: %w", err)
			}

			return publisher, subscriber, []closer{publisher, subscriber}, nil
		}
	}
}

// WithRabbitMQ selects RabbitMQ as the transport, connecting to uri.
func WithRabbitMQ(uri string) Option {
	return func(c *config) {
		c.build = func(cfg *config) (message.Publisher, message.Subscriber, []closer, error) {
			if strings.TrimSpace(uri) == "" {
				return nil, nil, nil, errors.New("pubsub: missing rabbitmq uri")
			}

			amqpConfig := amqp.NewDurablePubSubConfig(
				uri,
				amqp.GenerateQueueNameTopicNameWithSuffix(cfg.consumerGroup),
			)

			publisher, err := amqp.NewPublisher(amqpConfig, cfg.logger)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("pubsub: create rabbitmq publisher: %w", err)
			}

			subscriber, err := amqp.NewSubscriber(amqpConfig, cfg.logger)
			if err != nil {
				_ = publisher.Close()
				return nil, nil, nil, fmt.Errorf("pubsub: create rabbitmq subscriber: %w", err)
			}

			return publisher, subscriber, []closer{publisher, subscriber}, nil
		}
	}
}

// --- Tuning options ---------------------------------------------------

// WithConsumerGroup sets the consumer group / queue-name suffix used by
// transports that support it (Redis Streams, Kafka, Postgres, RabbitMQ).
// Defaults to "hermes". Ignored by WithGoChannel.
func WithConsumerGroup(group string) Option {
	return func(c *config) { c.consumerGroup = group }
}

// WithLogger sets the Watermill logger used by the underlying transport.
// Defaults to a no-op logger.
func WithLogger(logger watermill.LoggerAdapter) Option {
	return func(c *config) {
		if logger != nil {
			c.logger = logger
		}
	}
}

// WithTracer overrides the OpenTelemetry tracer used for publish/process
// spans. Defaults to otel.Tracer("pubsub").
func WithTracer(tracer trace.Tracer) Option {
	return func(c *config) {
		if tracer != nil {
			c.tracer = tracer
		}
	}
}

// WithPropagator overrides the propagator used to inject/extract trace
// context into message metadata. Defaults to the global propagator.
func WithPropagator(propagator propagation.TextMapPropagator) Option {
	return func(c *config) {
		if propagator != nil {
			c.propagator = propagator
		}
	}
}

// New builds a Bus from the given options. Exactly one driver option
// (WithGoChannel, WithRedisStream, WithKafka, WithPostgres, WithRabbitMQ)
// must be provided; New returns an error if none is given.
func New(opts ...Option) (Bus, error) {
	cfg := &config{
		logger:        watermill.NopLogger{},
		tracer:        otel.Tracer(tracerName),
		propagator:    otel.GetTextMapPropagator(),
		consumerGroup: "hermes",
	}

	for _, opt := range opts {
		if opt != nil {
			opt(cfg)
		}
	}

	if cfg.build == nil {
		return nil, errors.New("pubsub: a driver option is required (WithGoChannel, WithRedisStream, WithKafka, WithPostgres, or WithRabbitMQ)")
	}

	publisher, subscriber, closers, err := cfg.build(cfg)
	if err != nil {
		return nil, err
	}

	return &watermillBus{
		publisher:  publisher,
		subscriber: subscriber,
		closers:    closers,
		tracer:     cfg.tracer,
		propagator: cfg.propagator,
	}, nil
}
