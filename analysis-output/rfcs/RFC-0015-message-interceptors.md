# RFC-0015: Message Interceptors/Middleware

**Status:** Draft 📝
**Author:** Technical Analysis Team
**Created:** 2025-11-20
**Priority:** P1 (Strategic - Extensibility)

## Summary

Implement a message interceptor/middleware pattern to enable cross-cutting concerns (logging, metrics, tracing, encryption, validation) without modifying core client code.

## Motivation

### Current State

No way to hook into message pipeline:

```go
// Want to: add tracing, logging, encryption, validation
// Reality: Must wrap every Produce/ReadMessage call manually
p.Produce(msg, nil) // No hook for interceptors
```

### Problems

1. **No Extensibility**: Can't add behavior without forking
2. **Boilerplate**: Same tracing/logging code in every service
3. **No Composition**: Can't chain behaviors (trace + encrypt + log)
4. **Hard to Test**: Cross-cutting concerns coupled to business logic

## Detailed Design

### Interceptor Interface

```go
// ProducerInterceptor intercepts messages before/after producing
type ProducerInterceptor interface {
    // OnSend is called before producing
    OnSend(msg *Message) (*Message, error)

    // OnAcknowledgement is called after delivery report
    OnAcknowledgement(msg *Message, err error) error
}

// ConsumerInterceptor intercepts messages on consume
type ConsumerInterceptor interface {
    // OnConsume is called after message is received
    OnConsume(msg *Message) (*Message, error)

    // OnCommit is called before offset commit
    OnCommit(msg *Message) error
}
```

### Example: Tracing Interceptor

```go
type TracingInterceptor struct {
    tracer trace.Tracer
}

func (t *TracingInterceptor) OnSend(msg *Message) (*Message, error) {
    ctx := context.Background()
    ctx, span := t.tracer.Start(ctx, "kafka.produce")
    defer span.End()

    // Inject trace context into message headers
    propagator.Inject(ctx, &msg.Headers)

    span.SetAttributes(
        attribute.String("messaging.destination", *msg.TopicPartition.Topic),
        attribute.Int("messaging.message_size", len(msg.Value)),
    )

    return msg, nil
}

func (t *TracingInterceptor) OnAcknowledgement(msg *Message, err error) error {
    // Record metrics on delivery
    return nil
}
```

### Example: Encryption Interceptor

```go
type EncryptionInterceptor struct {
    cipher Cipher
}

func (e *EncryptionInterceptor) OnSend(msg *Message) (*Message, error) {
    // Encrypt message value
    encrypted, err := e.cipher.Encrypt(msg.Value)
    if err != nil {
        return nil, err
    }

    msg.Value = encrypted
    msg.Headers = append(msg.Headers, Header{
        Key:   "encrypted",
        Value: []byte("true"),
    })

    return msg, nil
}

func (e *EncryptionInterceptor) OnConsume(msg *Message) (*Message, error) {
    // Check if encrypted
    for _, h := range msg.Headers {
        if h.Key == "encrypted" && string(h.Value) == "true" {
            decrypted, err := e.cipher.Decrypt(msg.Value)
            if err != nil {
                return nil, err
            }
            msg.Value = decrypted
            break
        }
    }
    return msg, nil
}
```

### Producer with Interceptors

```go
producer, _ := kafka.NewProducer(&kafka.ConfigMap{...})

// Add interceptors
producer.AddInterceptor(&TracingInterceptor{...})
producer.AddInterceptor(&EncryptionInterceptor{...})
producer.AddInterceptor(&LoggingInterceptor{...})

// Produce - interceptors run automatically
producer.Produce(msg, nil)
// -> OnSend(tracing) -> OnSend(encryption) -> OnSend(logging) -> produce
```

## Benefits

1. **Extensibility**: Add behaviors without forking
2. **Composability**: Chain multiple interceptors
3. **Reusability**: Share interceptors across services
4. **Testability**: Test interceptors in isolation

**Effort:** 3 weeks
**Impact:** High (enables ecosystem of plugins)
