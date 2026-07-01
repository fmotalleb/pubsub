# pubsub

A small, transport-agnostic publish/subscribe abstraction on top of
[Watermill](https://watermill.io/), with built-in OpenTelemetry tracing
(trace context is propagated through message metadata automatically).

Supported transports: in-memory (go channel), Redis Streams, Kafka,
Postgres, and RabbitMQ.

## Install

```bash
go get github.com/fmotalleb/pubsub
```

Then update the module path in `go.mod` / your imports to wherever you
actually host this repo, and run `go mod tidy`.

## Usage

Construct a `Bus` with `New`, picking exactly one driver option:

```go
bus, err := pubsub.New(
  pubsub.WithRedisStream(redisClient),
  pubsub.WithConsumerGroup("orders-service"),
  pubsub.WithLogger(myLogger),
)
if err != nil {
  log.Fatal(err)
}
defer bus.Close()

err = bus.Publish(ctx, "orders.created", payload)

err = bus.Subscribe(ctx, "orders.created", func(ctx context.Context, payload []byte) error {
  // handle the message; a returned error nacks it
  return nil
})
```

### Driver options (pick exactly one)

| Option                         | Transport                            |
|--------------------------------|--------------------------------------|
| `WithGoChannel()`              | In-memory, for tests/single-process  |
| `WithRedisStream(client)`      | Redis Streams                        |
| `WithKafka(brokers)`           | Kafka                                |
| `WithPostgres(db)`             | Postgres                             |
| `WithRabbitMQ(uri)`            | RabbitMQ                             |

### Tuning options (optional, any order)

- `WithConsumerGroup(group)` — consumer group / queue suffix (default `"hermes"`; ignored by `WithGoChannel`)
- `WithLogger(logger)` — Watermill logger (default: no-op)
- `WithTracer(tracer)` — OpenTelemetry tracer (default: `otel.Tracer("pubsub")`)
- `WithPropagator(propagator)` — trace-context propagator (default: global propagator)

`New` returns an error if no driver option is supplied.
