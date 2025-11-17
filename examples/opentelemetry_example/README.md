# OpenTelemetry Tracing Example

This example demonstrates how to use OpenTelemetry distributed tracing with the Confluent Kafka Go client to trace message flow across producer and consumer services.

## Overview

OpenTelemetry integration enables:
- **Trace Context Propagation**: Automatically injects and extracts trace context via message headers
- **Distributed Tracing**: Track messages across services with parent-child span relationships
- **Observability**: View complete request traces in tools like Jaeger or Zipkin

## Prerequisites

1. **Kafka Broker**: Running locally or accessible via network
   ```bash
   # Using Docker
   docker run -d --name kafka -p 9092:9092 \
     apache/kafka:latest
   ```

2. **Jaeger (Tracing Backend)**: For visualizing traces
   ```bash
   # Using Docker
   docker run -d --name jaeger \
     -p 16686:16686 \
     -p 14268:14268 \
     jaegertracing/all-in-one:latest
   ```

3. **Go Dependencies**:
   ```bash
   go mod download
   ```

## Building

```bash
cd examples/opentelemetry_example
go build
```

## Running

### 1. Start Jaeger (if not already running)

```bash
docker run -d --name jaeger \
  -p 16686:16686 \
  -p 14268:14268 \
  jaegertracing/all-in-one:latest
```

Access Jaeger UI at: http://localhost:16686

### 2. Run the Producer

In one terminal:

```bash
./opentelemetry_example localhost:9092 otel-test producer
```

This will:
- Create a root span simulating an HTTP request
- Produce 5 messages to the `otel-test` topic
- Inject trace context into each message's headers
- Export spans to Jaeger

### 3. Run the Consumer

In another terminal:

```bash
./opentelemetry_example localhost:9092 otel-test consumer
```

This will:
- Subscribe to the `otel-test` topic
- Extract trace context from message headers
- Create consumer spans linked to producer spans
- Process messages with full trace context
- Export spans to Jaeger

### 4. View Traces in Jaeger

1. Open http://localhost:16686 in your browser
2. Select service: `kafka-producer-example` or `kafka-consumer-example`
3. Click "Find Traces"
4. Click on a trace to see the complete flow:
   - HTTP Request (root span)
   - Process Order (producer span)
   - Kafka Produce (producer span)
   - Kafka Consume (consumer span)
   - Process Message (consumer span)

## How It Works

### Producer

```go
// Enable OpenTelemetry in producer config
p, err := kafka.NewProducer(&kafka.ConfigMap{
    "bootstrap.servers": "localhost:9092",
    "go.otel.enabled":   true,  // Enable tracing
})

// Create context with span
ctx, span := tracer.Start(context.Background(), "produce-message")
defer span.End()

// ProduceWithContext injects trace context into message headers
err = p.ProduceWithContext(ctx, &kafka.Message{
    TopicPartition: kafka.TopicPartition{Topic: &topic},
    Value:          []byte("Hello World"),
}, nil)
```

### Consumer

```go
// Enable OpenTelemetry in consumer config
c, err := kafka.NewConsumer(&kafka.ConfigMap{
    "bootstrap.servers": "localhost:9092",
    "group.id":          "my-group",
    "go.otel.enabled":   true,  // Enable tracing
})

// ReadMessageWithContext extracts trace context and creates consumer span
msg, ctx, span, err := c.ReadMessageWithContext(context.Background(), -1)
defer span.End()

// ctx now contains the trace context from the producer
// Process message with full trace context
processMessage(ctx, msg)
```

## Trace Context Headers

The trace context is propagated using the W3C Trace Context standard via message headers:

- `traceparent`: Contains trace ID, span ID, and sampling decision
  - Format: `00-{trace-id}-{span-id}-{flags}`
  - Example: `00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01`

- `tracestate`: Optional vendor-specific trace data

## Configuration

### Producer Configuration

```go
&kafka.ConfigMap{
    "bootstrap.servers": "localhost:9092",
    "go.otel.enabled":   true,  // Enable OpenTelemetry (default: false)
}
```

### Consumer Configuration

```go
&kafka.ConfigMap{
    "bootstrap.servers": "localhost:9092",
    "group.id":          "my-group",
    "go.otel.enabled":   true,  // Enable OpenTelemetry (default: false)
}
```

## Semantic Conventions

The implementation follows OpenTelemetry semantic conventions for messaging:

### Span Attributes

**Producer Spans:**
- `messaging.system`: "kafka"
- `messaging.destination.name`: Topic name
- `messaging.destination.kind`: "topic"
- `messaging.operation`: "publish"
- `messaging.kafka.destination.partition`: Partition number (if set)

**Consumer Spans:**
- `messaging.system`: "kafka"
- `messaging.destination.name`: Topic name
- `messaging.destination.kind`: "topic"
- `messaging.operation`: "receive"
- `messaging.kafka.destination.partition`: Partition number
- `messaging.kafka.message.offset`: Message offset
- `messaging.kafka.consumer.group`: Consumer group ID

### Span Names

- Producer: `{topic-name} publish`
- Consumer: `{topic-name} receive`

## Performance Impact

OpenTelemetry tracing has minimal performance impact:
- Overhead: < 5% (mostly from header serialization)
- Configurable sampling rates
- Opt-in feature (disabled by default)

## Troubleshooting

### "Failed to initialize tracer: connection refused"

Jaeger is not running. Start Jaeger:
```bash
docker run -d --name jaeger -p 16686:16686 -p 14268:14268 jaegertracing/all-in-one:latest
```

### No traces visible in Jaeger

1. Check Jaeger is accessible: http://localhost:16686
2. Verify `go.otel.enabled: true` in both producer and consumer configs
3. Ensure messages are being produced and consumed
4. Check for errors in application logs

### Trace context not propagated

1. Verify producer is using `ProduceWithContext(ctx, ...)`
2. Verify consumer is using `ReadMessageWithContext(ctx, ...)`
3. Check message headers contain `traceparent`
4. Ensure same propagator is used (W3C TraceContext)

## References

- [OpenTelemetry Go Documentation](https://opentelemetry.io/docs/instrumentation/go/)
- [W3C Trace Context Specification](https://www.w3.org/TR/trace-context/)
- [OpenTelemetry Semantic Conventions for Messaging](https://opentelemetry.io/docs/specs/semconv/messaging/)
- [Jaeger Documentation](https://www.jaegertracing.io/docs/)
