# RFC-0011: Context-Aware APIs Throughout

**Status:** Draft 📝
**Author:** Technical Analysis Team
**Created:** 2025-11-20
**Priority:** P0 (Critical - Go Best Practice)

## Summary

Add `context.Context` support throughout all Producer, Consumer, and AdminClient APIs to enable proper cancellation, timeout propagation, and distributed tracing integration following Go best practices.

## Motivation

### Current State

Many critical APIs don't accept `context.Context`:

```go
// Producer - no context support
p.Produce(&kafka.Message{...}, nil)

// Consumer - no context on ReadMessage
msg, err := c.ReadMessage(timeout)

// AdminClient - uses context but inconsistently
topics, err := a.CreateTopics(ctx, specs) // ✅ Has context
```

### Problems

1. **No Cancellation**: Cannot cancel in-flight operations
2. **No Timeout Propagation**: Timeouts must be configured separately
3. **No Trace Context**: Cannot propagate OpenTelemetry trace context
4. **Not Idiomatic Go**: Violates Go best practices (context as first param)
5. **Resource Leaks**: Blocked operations can't be cancelled

### User Impact

**Production Issues:**
- Cannot implement request deadlines from HTTP handlers
- Blocked consumer reads during graceful shutdown
- No way to cancel long-running admin operations
- Trace context breaks at Kafka boundary

## Detailed Design

### Producer API

```go
// Current (no context)
func (p *Producer) Produce(msg *Message, deliveryChan chan Event) error

// Proposed (with context)
func (p *Producer) ProduceWithContext(ctx context.Context, msg *Message, deliveryChan chan Event) error {
    // Check context before producing
    select {
    case <-ctx.Done():
        return ctx.Err()
    default:
    }

    // Produce with timeout from context
    deadline, ok := ctx.Deadline()
    if ok {
        // Use deadline for librdkafka timeout
    }

    // Propagate trace context to message headers
    if span := trace.SpanFromContext(ctx); span.IsRecording() {
        // Inject trace context into headers
        propagator.Inject(ctx, &msg.Headers)
    }

    return p.produce(msg, deliveryChan)
}
```

### Consumer API

```go
// Current (timeout as int)
func (c *Consumer) ReadMessage(timeout time.Duration) (*Message, error)

// Proposed (with context)
func (c *Consumer) ReadMessageContext(ctx context.Context) (*Message, error) {
    // Respect context cancellation
    go func() {
        <-ctx.Done()
        // Wake up blocked poll
        c.wakeup()
    }()

    msg, err := c.poll(ctx)
    if err != nil {
        return nil, err
    }

    // Extract trace context from message headers
    ctx = propagator.Extract(ctx, msg.Headers)
    msg.Context = ctx // Store context in message

    return msg, nil
}
```

### AdminClient API

All admin operations already accept context, but need consistency:

```go
// Ensure all operations check context.Done()
func (a *AdminClient) CreateTopics(ctx context.Context, topics []TopicSpecification, options ...CreateTopicsAdminOption) ([]TopicResult, error) {
    select {
    case <-ctx.Done():
        return nil, ctx.Err()
    default:
    }
    // ... existing implementation
}
```

## Implementation Plan

**Phase 1: Add New Context APIs (Week 1-2)**
- Add `ProduceWithContext()`, `ReadMessageContext()`
- Mark old APIs as "prefer context version"
- Full backward compatibility

**Phase 2: Context Propagation (Week 3)**
- Integrate with OpenTelemetry
- Extract/inject trace context from headers
- Add context to Event structs

**Phase 3: Documentation & Examples (Week 4)**
- Update all examples to use context APIs
- Migration guide
- Best practices doc

**Phase 4: Deprecation Path (v3.0)**
- Deprecate non-context versions
- v3.0: Make context required

## Backward Compatibility

✅ **100% Backward Compatible**
- Add new methods alongside existing ones
- Existing code continues to work
- Gradual migration path

## Benefits

1. **Idiomatic Go**: Follows Go best practices
2. **Cancellation**: Proper request cancellation
3. **Tracing**: OpenTelemetry integration works properly
4. **Timeouts**: Propagate deadlines from HTTP handlers
5. **Graceful Shutdown**: Cancel operations during shutdown

## Success Criteria

- ✅ All Producer/Consumer APIs have context versions
- ✅ Trace context propagated through Kafka
- ✅ Context cancellation works end-to-end
- ✅ Examples show context usage
- ✅ Zero breaking changes

**Effort:** 3-4 weeks
**Impact:** Critical (Go best practice, enables tracing)
