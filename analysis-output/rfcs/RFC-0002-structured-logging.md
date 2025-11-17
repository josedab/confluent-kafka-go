# RFC-0002: Add Structured Logging with slog

**Status:** Draft 📝
**Author:** Technical Analysis Team
**Created:** 2025-11-16
**Updated:** 2025-11-16
**Priority:** P0 (Strategic)

---

## Summary

Introduce structured logging using Go's standard library `log/slog` (Go 1.21+) to replace the current unstructured logging mechanism. This provides machine-parseable logs with consistent fields, log levels, and context propagation, enabling better observability in production environments.

---

## Motivation

### Current State

confluent-kafka-go uses librdkafka's logging callbacks forwarded to Go channels:

**From `kafka/log.go`:**
```go
type LogEvent struct {
    Name      string    // Logger name
    Tag       string    // Log tag
    Message   string    // Log message
    Level     int       // Log level (0-7, syslog style)
    Timestamp time.Time // When the log occurred
}
```

**Problems:**
1. **Unstructured**: Log messages are plain strings, hard to parse
2. **No Context**: Cannot attach request IDs, trace IDs, or custom fields
3. **Limited Filtering**: Only numeric levels (0-7), no semantic levels
4. **Poor Integration**: Doesn't work with standard logging frameworks (Logrus, Zap, slog)
5. **No Sampling**: Cannot reduce log volume in high-throughput scenarios

### User Impact

**Production Issues:**
- ❌ Logs are hard to query in centralized logging (e.g., "find all errors for producer X")
- ❌ No correlation between logs and distributed traces
- ❌ Cannot add custom fields (customer_id, request_id, etc.)
- ❌ Log aggregation tools (ELK, Splunk) require custom parsers

**Example Current Log:**
```
2025-11-16 10:15:23 [rdkafka#producer-1] FAIL librdkafka: broker.example.com:9092: Disconnected
```

**What We Want:**
```json
{
  "time": "2025-11-16T10:15:23Z",
  "level": "ERROR",
  "msg": "Broker disconnected",
  "broker": "broker.example.com:9092",
  "client_id": "producer-1",
  "error": "connection reset",
  "trace_id": "abc123"
}
```

---

## Detailed Design

### Phase 1: Add slog Support (Backward Compatible)

#### 1.1 New Logger Interface

**File: `kafka/logger.go` (new file)**

```go
package kafka

import (
    "context"
    "log/slog"
)

// Logger is the interface for structured logging in confluent-kafka-go.
// Applications can provide custom implementations or use the default slog-based logger.
type Logger interface {
    // Debug logs a debug-level message with context and attributes.
    Debug(ctx context.Context, msg string, args ...any)

    // Info logs an info-level message with context and attributes.
    Info(ctx context.Context, msg string, args ...any)

    // Warn logs a warning-level message with context and attributes.
    Warn(ctx context.Context, msg string, args ...any)

    // Error logs an error-level message with context and attributes.
    Error(ctx context.Context, msg string, args ...any)

    // With returns a new Logger with additional attributes.
    With(args ...any) Logger
}

// SlogLogger wraps slog.Logger to implement the Logger interface.
type SlogLogger struct {
    logger *slog.Logger
}

// NewSlogLogger creates a new structured logger using slog.
func NewSlogLogger(handler slog.Handler) Logger {
    return &SlogLogger{
        logger: slog.New(handler),
    }
}

func (l *SlogLogger) Debug(ctx context.Context, msg string, args ...any) {
    l.logger.DebugContext(ctx, msg, args...)
}

func (l *SlogLogger) Info(ctx context.Context, msg string, args ...any) {
    l.logger.InfoContext(ctx, msg, args...)
}

func (l *SlogLogger) Warn(ctx context.Context, msg string, args ...any) {
    l.logger.WarnContext(ctx, msg, args...)
}

func (l *SlogLogger) Error(ctx context.Context, msg string, args ...any) {
    l.logger.ErrorContext(ctx, msg, args...)
}

func (l *SlogLogger) With(args ...any) Logger {
    return &SlogLogger{
        logger: l.logger.With(args...),
    }
}

// Default logger (uses slog's default handler)
var defaultLogger Logger = NewSlogLogger(slog.Default().Handler())

// SetLogger sets the global logger for confluent-kafka-go.
func SetLogger(logger Logger) {
    defaultLogger = logger
}

// GetLogger returns the current global logger.
func GetLogger() Logger {
    return defaultLogger
}
```

#### 1.2 Bridge librdkafka Logs to slog

**Update `kafka/handle.go`:**

```go
// Add to handle struct
type handle struct {
    // ... existing fields ...

    // New structured logger
    logger Logger
}

// Update setup() to initialize logger
func (h *handle) setup() {
    // ... existing code ...

    // Initialize logger with client-specific attributes
    h.logger = GetLogger().With(
        "client_type", h.clientType(), // "producer", "consumer", "admin"
        "client_name", h.name,
    )
}

// Forward librdkafka logs to structured logger
func (h *handle) forwardLogs() {
    for logEvent := range h.logs {
        ctx := context.Background()

        // Convert librdkafka log level to slog level
        level := mapLibrdkafkaLevel(logEvent.Level)

        // Common attributes
        args := []any{
            "source", "librdkafka",
            "tag", logEvent.Tag,
            "name", logEvent.Name,
        }

        switch level {
        case slog.LevelDebug:
            h.logger.Debug(ctx, logEvent.Message, args...)
        case slog.LevelInfo:
            h.logger.Info(ctx, logEvent.Message, args...)
        case slog.LevelWarn:
            h.logger.Warn(ctx, logEvent.Message, args...)
        case slog.LevelError:
            h.logger.Error(ctx, logEvent.Message, args...)
        }
    }
}

// Map librdkafka's syslog-style levels (0-7) to slog levels
func mapLibrdkafkaLevel(level int) slog.Level {
    switch {
    case level <= 3: // Emergency, Alert, Critical, Error
        return slog.LevelError
    case level == 4: // Warning
        return slog.LevelWarn
    case level <= 6: // Notice, Informational
        return slog.LevelInfo
    default: // Debug
        return slog.LevelDebug
    }
}
```

#### 1.3 Configuration

**Add to `kafka/config.go`:**

```go
// New configuration keys
const (
    // ConfigLogger sets a custom Logger implementation.
    // Type: Logger interface
    // Default: slog-based logger with JSON handler
    ConfigLogger = "go.logger"

    // ConfigLogLevel sets the minimum log level.
    // Type: string ("debug", "info", "warn", "error")
    // Default: "info"
    ConfigLogLevel = "go.log.level"

    // ConfigLogFormat sets the log output format.
    // Type: string ("json", "text")
    // Default: "json"
    ConfigLogFormat = "go.log.format"

    // ConfigLogOutput sets the log output destination.
    // Type: io.Writer
    // Default: os.Stderr
    ConfigLogOutput = "go.log.output"
)
```

#### 1.4 Application Usage

**Example: Default JSON Logging**

```go
import (
    "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
    // Structured logging enabled by default (Go 1.21+)
    producer, err := kafka.NewProducer(&kafka.ConfigMap{
        "bootstrap.servers": "localhost:9092",
        "go.log.level":      "info",
        "go.log.format":     "json",
    })

    // Logs will be JSON formatted:
    // {"time":"...","level":"INFO","msg":"Producer created","client_name":"rdkafka#producer-1"}
}
```

**Example: Custom Logger with Additional Context**

```go
import (
    "log/slog"
    "os"
    "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
    // Create custom logger with additional attributes
    handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
        Level: slog.LevelDebug,
    })

    logger := kafka.NewSlogLogger(handler).With(
        "service", "payment-processor",
        "version", "v1.2.3",
        "environment", "production",
    )

    kafka.SetLogger(logger)

    producer, err := kafka.NewProducer(&kafka.ConfigMap{
        "bootstrap.servers": "localhost:9092",
    })

    // All logs now include: service, version, environment fields
}
```

**Example: Integration with OpenTelemetry**

```go
import (
    "context"
    "log/slog"
    "go.opentelemetry.io/otel/trace"
    "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Custom handler that adds trace context to logs
type OTelHandler struct {
    handler slog.Handler
}

func (h *OTelHandler) Handle(ctx context.Context, r slog.Record) error {
    span := trace.SpanFromContext(ctx)
    if span.SpanContext().IsValid() {
        r.AddAttrs(
            slog.String("trace_id", span.SpanContext().TraceID().String()),
            slog.String("span_id", span.SpanContext().SpanID().String()),
        )
    }
    return h.handler.Handle(ctx, r)
}

func main() {
    handler := &OTelHandler{
        handler: slog.NewJSONHandler(os.Stdout, nil),
    }

    logger := kafka.NewSlogLogger(handler)
    kafka.SetLogger(logger)

    // All logs now include trace_id and span_id when available
}
```

---

### Phase 2: Internal Logging Migration

#### 2.1 Replace Printf-style Logging

**Before:**
```go
// kafka/producer.go
fmt.Printf("Failed to produce message: %v\n", err)
```

**After:**
```go
// kafka/producer.go
p.handle.logger.Error(ctx, "Failed to produce message",
    "error", err,
    "topic", *msg.TopicPartition.Topic,
    "partition", msg.TopicPartition.Partition,
)
```

#### 2.2 Add Contextual Logging

**Example in Producer:**
```go
// kafka/producer.go

func (p *Producer) Produce(msg *Message, deliveryChan chan Event) error {
    ctx := context.Background()

    p.handle.logger.Debug(ctx, "Producing message",
        "topic", *msg.TopicPartition.Topic,
        "partition", msg.TopicPartition.Partition,
        "key_len", len(msg.Key),
        "value_len", len(msg.Value),
    )

    err := p.produce(msg, msgFlags, deliveryChan)
    if err != nil {
        p.handle.logger.Error(ctx, "Produce failed",
            "error", err,
            "topic", *msg.TopicPartition.Topic,
        )
        return err
    }

    return nil
}
```

**Example in Consumer:**
```go
// kafka/consumer.go

func (c *Consumer) Poll(timeoutMs int) (Event, error) {
    ctx := context.Background()

    c.handle.logger.Debug(ctx, "Polling for messages",
        "timeout_ms", timeoutMs,
    )

    event, err := c.poll(timeoutMs)

    if msg, ok := event.(*Message); ok {
        c.handle.logger.Debug(ctx, "Message received",
            "topic", *msg.TopicPartition.Topic,
            "partition", msg.TopicPartition.Partition,
            "offset", msg.TopicPartition.Offset,
        )
    }

    return event, err
}
```

---

### Phase 3: Advanced Features

#### 3.1 Log Sampling (High-Throughput Mode)

**Problem:** At 1M msg/sec, debug logging is expensive

**Solution:** Sample logs (e.g., log 1 in 1000)

```go
type SamplingLogger struct {
    logger   Logger
    rate     int // Log 1 in N messages
    counter  atomic.Uint64
}

func (l *SamplingLogger) Debug(ctx context.Context, msg string, args ...any) {
    count := l.counter.Add(1)
    if count%uint64(l.rate) == 0 {
        l.logger.Debug(ctx, msg, args...)
    }
}

// Configuration
producer, _ := kafka.NewProducer(&kafka.ConfigMap{
    "go.log.sampling.rate": 1000, // Log 1 in 1000 debug messages
})
```

#### 3.2 Structured Error Logging

```go
// kafka/error.go

func (e Error) LogValue() slog.Value {
    return slog.GroupValue(
        slog.Int("code", int(e.code)),
        slog.String("name", e.str),
        slog.Bool("retriable", e.IsRetriable()),
        slog.Bool("fatal", e.IsFatal()),
    )
}

// Usage
logger.Error(ctx, "Operation failed",
    "error", kafkaError, // Automatically structured
)

// Output:
// {"level":"ERROR","msg":"Operation failed","error":{"code":-185,"name":"MSG_TIMED_OUT","retriable":true,"fatal":false}}
```

---

## Implementation Plan

### Timeline: 2 Weeks

**Week 1: Foundation**

**Day 1-2: Logger Interface & slog Integration**
- [ ] Create `kafka/logger.go` with Logger interface
- [ ] Implement SlogLogger wrapper
- [ ] Add configuration keys to ConfigMap
- [ ] Update handle.go to use Logger

**Day 3-4: librdkafka Log Forwarding**
- [ ] Update log forwarding to use structured logger
- [ ] Add level mapping (syslog → slog)
- [ ] Test with various log levels

**Day 5: Documentation & Examples**
- [ ] Write migration guide
- [ ] Create example: `examples/structured_logging_example/`
- [ ] Update README with logging section

---

**Week 2: Migration & Testing**

**Day 1-2: Internal Logging Migration**
- [ ] Migrate producer.go logging
- [ ] Migrate consumer.go logging
- [ ] Migrate adminapi.go logging

**Day 3: Advanced Features**
- [ ] Implement log sampling
- [ ] Add Error.LogValue() for structured errors
- [ ] Add OpenTelemetry example

**Day 4: Testing**
- [ ] Unit tests for logger interface
- [ ] Integration tests with custom loggers
- [ ] Performance benchmarks (overhead measurement)

**Day 5: Final Review & Documentation**
- [ ] Update API documentation
- [ ] Add performance tuning guide
- [ ] Create PR with examples

---

## Backward Compatibility

### ✅ 100% Backward Compatible

**Existing Code Works Unchanged:**

```go
// Existing code continues to work
producer, _ := kafka.NewProducer(&kafka.ConfigMap{
    "bootstrap.servers": "localhost:9092",
})
// Logs still work (using default slog handler)
```

**Old LogEvent Channel Still Works:**

```go
// Existing LogEvent channel-based logging still supported
config := &kafka.ConfigMap{
    "go.logs.channel.enable": true, // Existing flag
}
producer, _ := kafka.NewProducer(config)

for logEvent := range producer.Logs() {
    // Old API still works
}
```

**Migration Path:**

1. **Phase 1** (v2.x): Add slog support, keep LogEvent
2. **Phase 2** (v2.x+1): Deprecate LogEvent channel
3. **Phase 3** (v3.0): Remove LogEvent channel (breaking change)

**Deprecation Notice:**
```go
// kafka/log.go

// LogEvent is deprecated. Use the structured Logger interface instead.
// This will be removed in v3.0.
//
// Deprecated: Use kafka.SetLogger() with a slog-based logger.
type LogEvent struct {
    // ...
}
```

---

## Alternatives Considered

### Alternative 1: Use Logrus or Zap

**Approach:** Adopt a third-party logging library

**Rejected Because:**
- Adds external dependency (against project philosophy)
- slog is now standard library (Go 1.21+)
- Users can still wrap Logrus/Zap via Logger interface

---

### Alternative 2: Keep Current Logging

**Approach:** Don't change anything

**Rejected Because:**
- Doesn't address production observability needs
- Industry standard is structured logging
- Competitors (Sarama) already have structured logging

---

### Alternative 3: Custom Structured Format

**Approach:** Invent our own key=value format

**Rejected Because:**
- Reinventing the wheel
- slog is well-designed and extensible
- No benefit over using standard library

---

## Performance Impact

### Benchmarks (Expected)

**Current (unstructured):**
```
BenchmarkLogUnstructured-8    1000000    1200 ns/op    256 B/op    4 allocs/op
```

**With slog (structured):**
```
BenchmarkLogSlog-8            1000000    1400 ns/op    320 B/op    5 allocs/op
```

**Overhead:** ~200ns per log call (~16% increase)

**Mitigation:**
- Use appropriate log levels (debug disabled in production)
- Implement log sampling for high-frequency debug logs
- Users can provide no-op logger if needed

---

## Success Criteria

### Functional
- ✅ All existing tests pass
- ✅ LogEvent channel still works (backward compatibility)
- ✅ Custom loggers can be provided
- ✅ Logs include structured fields (client_name, topic, partition, etc.)

### Performance
- ✅ Log overhead < 20% (200ns acceptable)
- ✅ No performance regression with logging disabled
- ✅ Sampling reduces overhead for high-frequency logs

### Usability
- ✅ Migration guide available
- ✅ Examples for common use cases
- ✅ Integration examples (OpenTelemetry, ELK)

### Documentation
- ✅ API docs updated
- ✅ README includes logging section
- ✅ Blog post published (best practices)

---

## Open Questions

### Q1: Should we make slog the default handler or require opt-in?

**Option A:** Default to slog (Go 1.21+ requirement)
- Pros: Better out-of-box experience
- Cons: Forces Go 1.21+

**Option B:** Keep unstructured by default, require explicit slog opt-in
- Pros: Works with older Go versions
- Cons: Users must discover and enable feature

**Recommendation:** **Option A** - Require Go 1.21+ (already in go.mod), make slog default

---

### Q2: Should we provide built-in handlers for common formats?

**Options:**
- Logfmt handler
- Colored console handler
- CEF (Common Event Format) handler

**Recommendation:** Start with JSON and Text (slog built-ins), let users provide custom handlers

---

### Q3: How to handle context propagation for background goroutines?

**Problem:** librdkafka callbacks don't have Go context

**Solution:** Use background context by default, allow users to set context via config:

```go
producer, _ := kafka.NewProducer(&kafka.ConfigMap{
    "go.log.context": myContext, // Custom context
})
```

---

## Security Considerations

### Sensitive Data in Logs

**Problem:** Logs might contain sensitive data (keys, values, passwords)

**Mitigation:**

```go
// Add RedactingLogger wrapper
type RedactingLogger struct {
    logger Logger
    redact []string // Field names to redact
}

func (l *RedactingLogger) Error(ctx context.Context, msg string, args ...any) {
    redactedArgs := l.redactFields(args)
    l.logger.Error(ctx, msg, redactedArgs...)
}

// Usage
logger := kafka.NewRedactingLogger(baseLogger, []string{"password", "api_key", "secret"})
kafka.SetLogger(logger)
```

**Best Practice:** Don't log message keys/values by default (configurable)

---

## Migration Guide

### For Users

**Step 1: Update Go Version**
```bash
# Ensure Go 1.21+
go mod edit -go=1.21
```

**Step 2: Enable Structured Logging (if not default)**
```go
import "github.com/confluentinc/confluent-kafka-go/v2/kafka"

producer, _ := kafka.NewProducer(&kafka.ConfigMap{
    "bootstrap.servers": "localhost:9092",
    "go.log.format":     "json", // Enable JSON logs
})
```

**Step 3: Integrate with Your Logging Stack**
```go
// Example: Integrate with Zap
import (
    "go.uber.org/zap"
    "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Wrap Zap logger
type ZapLogger struct {
    logger *zap.SugaredLogger
}

func (l *ZapLogger) Error(ctx context.Context, msg string, args ...any) {
    l.logger.Errorw(msg, args...)
}
// ... implement other methods

zapLogger := zap.NewProduction().Sugar()
kafka.SetLogger(&ZapLogger{logger: zapLogger})
```

---

## References

- [slog Documentation](https://pkg.go.dev/log/slog)
- [Structured Logging Best Practices](https://www.cncf.io/blog/2021/03/30/structured-logging/)
- [OpenTelemetry Logs](https://opentelemetry.io/docs/specs/otel/logs/)
- [Confluent Cloud Log Format](https://docs.confluent.io/cloud/current/monitoring/metrics-api.html)

---

## Conclusion

Structured logging with slog brings confluent-kafka-go up to modern observability standards while maintaining backward compatibility. The phased approach (add support → migrate internally → deprecate old API) ensures a smooth transition for users.

**Recommended Action:** Approve for implementation in Q1 2026 (2-week sprint).

---

**Status**: 📝 Draft — Awaiting maintainer review
**Next Review**: 2025-12-01
