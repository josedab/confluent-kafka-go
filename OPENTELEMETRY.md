# OpenTelemetry Integration Guide

This guide explains how to use OpenTelemetry distributed tracing with the Confluent Kafka Go client to trace message flow across producer and consumer services.

## Table of Contents

- [Overview](#overview)
- [Getting Started](#getting-started)
- [Configuration](#configuration)
- [Producer Tracing](#producer-tracing)
- [Consumer Tracing](#consumer-tracing)
- [Best Practices](#best-practices)
- [Performance Considerations](#performance-considerations)
- [Troubleshooting](#troubleshooting)

## Overview

OpenTelemetry integration provides:

- **Automatic Trace Context Propagation**: Trace context is automatically injected into message headers by producers and extracted by consumers
- **Distributed Tracing**: Track messages across services with parent-child span relationships
- **Semantic Conventions**: Follows OpenTelemetry semantic conventions for messaging systems
- **Low Overhead**: Minimal performance impact (<5%)
- **Opt-in**: Disabled by default, enabled via configuration

## Getting Started

### Prerequisites

1. **OpenTelemetry SDK**: Install the OpenTelemetry Go SDK and exporter of your choice
   ```bash
   go get go.opentelemetry.io/otel
   go get go.opentelemetry.io/otel/sdk
   go get go.opentelemetry.io/otel/exporters/jaeger  # Or other exporter
   ```

2. **Tracing Backend**: Set up a tracing backend (Jaeger, Zipkin, etc.)
   ```bash
   # Example: Run Jaeger locally
   docker run -d --name jaeger \
     -p 16686:16686 \
     -p 14268:14268 \
     jaegertracing/all-in-one:latest
   ```

3. **Initialize OpenTelemetry**: Set up the tracer provider and propagator in your application
   ```go
   import (
       "go.opentelemetry.io/otel"
       "go.opentelemetry.io/otel/propagation"
       "go.opentelemetry.io/otel/sdk/trace"
   )

   // Initialize tracer provider (example with Jaeger)
   exporter, _ := jaeger.New(jaeger.WithCollectorEndpoint(...))
   tp := trace.NewTracerProvider(
       trace.WithBatcher(exporter),
   )
   otel.SetTracerProvider(tp)

   // Set W3C TraceContext propagator
   otel.SetTextMapPropagator(propagation.TraceContext{})
   ```

### Quick Start

**Producer with tracing:**

```go
import (
    "context"
    "github.com/confluentinc/confluent-kafka-go/v2/kafka"
    "go.opentelemetry.io/otel"
)

// Create producer with OTel enabled
p, _ := kafka.NewProducer(&kafka.ConfigMap{
    "bootstrap.servers": "localhost:9092",
    "go.otel.enabled":   true,  // Enable tracing
})
defer p.Close()

// Get tracer
tracer := otel.Tracer("my-service")
ctx, span := tracer.Start(context.Background(), "process-order")
defer span.End()

// Produce with context - trace context will be injected
msg := &kafka.Message{
    TopicPartition: kafka.TopicPartition{Topic: &topic},
    Value:          []byte("order data"),
}
p.ProduceWithContext(ctx, msg, nil)
```

**Consumer with tracing:**

```go
// Create consumer with OTel enabled
c, _ := kafka.NewConsumer(&kafka.ConfigMap{
    "bootstrap.servers": "localhost:9092",
    "group.id":          "my-group",
    "go.otel.enabled":   true,  // Enable tracing
})
defer c.Close()

// Subscribe to topic
c.SubscribeTopics([]string{"orders"}, nil)

// Consume with context - trace context will be extracted
msg, ctx, span, _ := c.ReadMessageWithContext(context.Background(), -1)
defer span.End()

// Process message with trace context
processOrder(ctx, msg)
```

## Configuration

### Enable OpenTelemetry

OpenTelemetry tracing is disabled by default and must be explicitly enabled:

**Producer:**
```go
p, err := kafka.NewProducer(&kafka.ConfigMap{
    "bootstrap.servers": "localhost:9092",
    "go.otel.enabled":   true,  // Default: false
})
```

**Consumer:**
```go
c, err := kafka.NewConsumer(&kafka.ConfigMap{
    "bootstrap.servers": "localhost:9092",
    "group.id":          "my-group",
    "go.otel.enabled":   true,  // Default: false
})
```

### Propagator Configuration

The library uses the globally configured OpenTelemetry text map propagator. Set this in your application initialization:

```go
import "go.opentelemetry.io/otel"
import "go.opentelemetry.io/otel/propagation"

// W3C TraceContext (recommended)
otel.SetTextMapPropagator(propagation.TraceContext{})

// Or composite propagator for multiple formats
otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
    propagation.TraceContext{},
    propagation.Baggage{},
))
```

## Producer Tracing

### Using ProduceWithContext

The `ProduceWithContext` method creates a producer span and injects trace context into message headers:

```go
ctx, span := tracer.Start(context.Background(), "send-notification")
defer span.End()

msg := &kafka.Message{
    TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
    Key:            []byte("user-123"),
    Value:          []byte("notification data"),
}

err := producer.ProduceWithContext(ctx, msg, nil)
if err != nil {
    span.RecordError(err)
}
```

### Producer Span Attributes

Producer spans automatically include these attributes:

- `messaging.system`: "kafka"
- `messaging.destination.name`: Topic name
- `messaging.destination.kind`: "topic"
- `messaging.operation`: "publish"
- `messaging.kafka.destination.partition`: Partition (if specified)

### Span Naming

Producer spans are named: `{topic-name} publish`

Example: `orders publish`

### Delivery Reports

Delivery reports are handled separately from the produce span. The produce span ends immediately after the message is queued:

```go
// Handle delivery reports
go func() {
    for e := range producer.Events() {
        switch ev := e.(type) {
        case *kafka.Message:
            if ev.TopicPartition.Error != nil {
                // Handle delivery error
            }
        }
    }
}()
```

### Backward Compatibility

The original `Produce` method remains unchanged:

```go
// Old API - still works, no tracing
producer.Produce(msg, nil)

// New API - with tracing
producer.ProduceWithContext(ctx, msg, nil)
```

## Consumer Tracing

### Using ReadMessageWithContext

The `ReadMessageWithContext` method extracts trace context from message headers and creates a consumer span:

```go
msg, ctx, span, err := consumer.ReadMessageWithContext(context.Background(), -1)
if err != nil {
    // Handle error
}
defer span.End()

// ctx now contains the extracted trace context
processMessage(ctx, msg)
```

### Consumer Span Attributes

Consumer spans automatically include these attributes:

- `messaging.system`: "kafka"
- `messaging.destination.name`: Topic name
- `messaging.destination.kind`: "topic"
- `messaging.operation`: "receive"
- `messaging.kafka.destination.partition`: Partition
- `messaging.kafka.message.offset`: Message offset
- `messaging.kafka.consumer.group`: Consumer group ID

### Span Naming

Consumer spans are named: `{topic-name} receive`

Example: `orders receive`

### Span Lifecycle

The consumer span should be ended after message processing is complete:

```go
msg, ctx, span, err := consumer.ReadMessageWithContext(context.Background(), -1)
if err != nil {
    return err
}

// Process message
processMessage(ctx, msg)

// Commit offset
consumer.CommitMessage(msg)

// End span after processing
span.End()
```

### Creating Child Spans

Create child spans for message processing steps:

```go
msg, ctx, span, _ := consumer.ReadMessageWithContext(context.Background(), -1)
defer span.End()

// Create child span for validation
_, validateSpan := tracer.Start(ctx, "validate-message")
if err := validate(msg); err != nil {
    validateSpan.RecordError(err)
    validateSpan.End()
    return err
}
validateSpan.End()

// Create child span for database operation
_, dbSpan := tracer.Start(ctx, "save-to-database")
if err := saveToDb(msg); err != nil {
    dbSpan.RecordError(err)
    dbSpan.End()
    return err
}
dbSpan.End()
```

### Backward Compatibility

The original `ReadMessage` method remains unchanged:

```go
// Old API - still works, no tracing
msg, err := consumer.ReadMessage(-1)

// New API - with tracing
msg, ctx, span, err := consumer.ReadMessageWithContext(context.Background(), -1)
```

## Best Practices

### 1. Always End Spans

Use defer to ensure spans are ended:

```go
msg, ctx, span, _ := consumer.ReadMessageWithContext(context.Background(), -1)
defer span.End()
```

### 2. Record Errors

Record errors on spans for better observability:

```go
if err := processMessage(msg); err != nil {
    span.RecordError(err)
    span.SetStatus(codes.Error, err.Error())
    return err
}
```

### 3. Add Custom Attributes

Add application-specific attributes to spans:

```go
import "go.opentelemetry.io/otel/attribute"

span.SetAttributes(
    attribute.String("user.id", userID),
    attribute.Int("order.amount", amount),
)
```

### 4. Use Sampling

Configure sampling to reduce overhead in high-throughput scenarios:

```go
import "go.opentelemetry.io/otel/sdk/trace"

tp := trace.NewTracerProvider(
    trace.WithSampler(trace.TraceIDRatioBased(0.1)), // Sample 10% of traces
)
```

### 5. Context Propagation

Always pass context through your application:

```go
func handleOrder(ctx context.Context, msg *kafka.Message) error {
    // Pass context to child functions
    if err := validateOrder(ctx, msg); err != nil {
        return err
    }
    return saveOrder(ctx, msg)
}
```

### 6. Batch Processing

For batch consumers, create a parent span for the batch and child spans for each message:

```go
batchCtx, batchSpan := tracer.Start(context.Background(), "process-batch")
defer batchSpan.End()

for i := 0; i < batchSize; i++ {
    msg, msgCtx, msgSpan, _ := consumer.ReadMessageWithContext(batchCtx, -1)

    // Process message in goroutine
    go func(ctx context.Context, m *kafka.Message, s trace.Span) {
        defer s.End()
        processMessage(ctx, m)
    }(msgCtx, msg, msgSpan)
}
```

## Performance Considerations

### Overhead

OpenTelemetry tracing has minimal performance impact:
- **Header injection/extraction**: ~1-2% overhead
- **Span creation**: Negligible with in-memory exporters
- **Network export**: Depends on exporter configuration (batching recommended)
- **Overall**: Typically <5% end-to-end

### Optimization Tips

1. **Use Batching**: Configure batching for span exporters
   ```go
   tp := trace.NewTracerProvider(
       trace.WithBatcher(exporter), // Batches spans before export
   )
   ```

2. **Adjust Sampling**: Use sampling for high-throughput applications
   ```go
   trace.WithSampler(trace.TraceIDRatioBased(0.1)) // 10% sampling
   ```

3. **Disable When Not Needed**: Keep `go.otel.enabled: false` in environments where tracing isn't needed

4. **Use Async Exporters**: Choose exporters that export spans asynchronously

## Troubleshooting

### Trace Context Not Propagated

**Symptoms**: Consumer spans don't link to producer spans

**Solutions**:
1. Verify `go.otel.enabled: true` in both producer and consumer
2. Check producer uses `ProduceWithContext(ctx, ...)`
3. Check consumer uses `ReadMessageWithContext(ctx, ...)`
4. Verify same propagator is configured:
   ```go
   otel.SetTextMapPropagator(propagation.TraceContext{})
   ```

### No Spans Visible in Backend

**Symptoms**: Application runs but no traces appear

**Solutions**:
1. Verify tracer provider is initialized
2. Check exporter configuration and endpoint
3. Ensure spans are being ended (use defer)
4. Check for export errors in logs
5. Verify backend is accessible

### High Memory Usage

**Symptoms**: Memory usage increases over time

**Solutions**:
1. Ensure all spans are ended (check for span leaks)
2. Configure span limits:
   ```go
   trace.WithSpanLimits(trace.SpanLimits{
       AttributeCountLimit: 128,
   })
   ```
3. Use sampling to reduce span volume
4. Check exporter batching configuration

### Headers Too Large

**Symptoms**: Kafka rejects messages with "Message too large"

**Solutions**:
1. Check propagator configuration (some add large headers)
2. Use W3C TraceContext (minimal header size)
3. Increase `message.max.bytes` in broker config if needed

## Examples

See the [examples/opentelemetry_example](examples/opentelemetry_example) directory for a complete working example with:
- Producer and consumer with tracing
- Jaeger integration
- Parent-child span relationships
- Error handling
- Comprehensive documentation

## References

- [OpenTelemetry Go Documentation](https://opentelemetry.io/docs/instrumentation/go/)
- [W3C Trace Context Specification](https://www.w3.org/TR/trace-context/)
- [OpenTelemetry Semantic Conventions for Messaging](https://opentelemetry.io/docs/specs/semconv/messaging/kafka/)
- [Jaeger Documentation](https://www.jaegertracing.io/docs/)
- [Zipkin Documentation](https://zipkin.io/pages/quickstart)
