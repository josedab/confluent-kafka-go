# RFC-0010: Add OpenTelemetry Integration

**Status:** Draft 📝
**Author:** Technical Analysis Team
**Created:** 2025-11-16
**Priority:** P2 (Advanced Observability)

## Summary

Add OpenTelemetry instrumentation for distributed tracing of Kafka produce and consume operations, enabling end-to-end request tracking across microservices.

## Motivation

**Use Case:** Track message flow across services

```
Service A --produce--> Kafka --consume--> Service B
   |                                          |
   trace_id=abc123                           trace_id=abc123
```

**Current State:** No trace propagation
**Desired:** Automatic trace context injection and extraction

## Detailed Design

### Trace Context Propagation

**Producer (Inject trace context):**
```go
import (
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/propagation"
)

func (p *Producer) Produce(ctx context.Context, msg *Message) error {
    tracer := otel.Tracer("confluent-kafka-go/producer")
    ctx, span := tracer.Start(ctx, "kafka.produce",
        trace.WithAttributes(
            attribute.String("messaging.system", "kafka"),
            attribute.String("messaging.destination", *msg.TopicPartition.Topic),
        ),
    )
    defer span.End()

    // Inject trace context into message headers
    propagator := otel.GetTextMapPropagator()
    carrier := NewMessageCarrier(msg)
    propagator.Inject(ctx, carrier)

    return p.produce(msg)
}

// MessageCarrier implements TextMapCarrier for Kafka message headers
type MessageCarrier struct {
    msg *Message
}

func (c *MessageCarrier) Set(key, value string) {
    if c.msg.Headers == nil {
        c.msg.Headers = []Header{}
    }
    c.msg.Headers = append(c.msg.Headers, Header{Key: key, Value: []byte(value)})
}
```

**Consumer (Extract trace context):**
```go
func (c *Consumer) Poll(ctx context.Context, timeout int) (Event, error) {
    msg, err := c.poll(timeout)
    if err != nil {
        return nil, err
    }

    // Extract trace context from message headers
    propagator := otel.GetTextMapPropagator()
    carrier := NewMessageCarrier(msg)
    ctx = propagator.Extract(ctx, carrier)

    tracer := otel.Tracer("confluent-kafka-go/consumer")
    _, span := tracer.Start(ctx, "kafka.consume",
        trace.WithAttributes(
            attribute.String("messaging.system", "kafka"),
            attribute.String("messaging.source", *msg.TopicPartition.Topic),
            attribute.Int64("messaging.kafka.offset", int64(msg.TopicPartition.Offset)),
        ),
    )
    defer span.End()

    return msg, nil
}
```

### Configuration

**Enable tracing:**
```go
producer, _ := kafka.NewProducer(&kafka.ConfigMap{
    "bootstrap.servers": "localhost:9092",
    "go.otel.enabled":   true, // Enable OTel
})
```

## Implementation Plan

**Week 1-2: Core Tracing**
- Implement trace injection (producer)
- Implement trace extraction (consumer)
- Add span attributes

**Week 3: Testing**
- Integration tests with Jaeger
- Performance overhead measurement
- Example with full trace

**Week 4: Documentation**
- OTel integration guide
- Jaeger setup example
- Best practices

## Performance Impact

**Expected Overhead:** <5% (trace context is small)
**Mitigation:** Opt-in feature, sampling configurable

## Success Criteria

- ✅ Traces visible in Jaeger/Zipkin
- ✅ Parent-child span relationships correct
- ✅ Performance overhead <5%

**Effort:** 3-4 weeks
**Impact:** High (enterprise observability)
