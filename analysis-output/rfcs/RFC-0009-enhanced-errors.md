# RFC-0009: Enhanced Error Messages with Context

**Status:** Draft 📝
**Author:** Technical Analysis Team
**Created:** 2025-11-16
**Priority:** P1 (Quick Win)

## Summary

Enhance error messages with actionable context, configuration hints, and troubleshooting steps to improve user experience when things go wrong.

## Motivation

**Current Errors:**
```go
Error: Local: Queue full
```

**Problems:**
- What queue? Producer? Consumer?
- Why is it full?
- How to fix?

**Better Error:**
```go
Error: Producer queue full (100000/100000 messages)
  
Possible causes:
  1. Producing faster than broker can accept
  2. Network issues to broker
  3. Queue size too small

Solutions:
  - Increase queue size: "queue.buffering.max.messages"
  - Add backpressure handling in application
  - Check broker health and network latency

Configuration:
  Current: queue.buffering.max.messages=100000
  Suggested: queue.buffering.max.messages=200000
```

## Detailed Design

### Enhanced Error Type

```go
// kafka/error.go

type EnhancedError struct {
    Code       ErrorCode
    Message    string
    Context    map[string]interface{} // Additional context
    Causes     []string                // Possible causes
    Solutions  []string                // How to fix
    ConfigHint *ConfigHint             // Configuration suggestion
}

type ConfigHint struct {
    Key           string
    CurrentValue  string
    SuggestedValue string
    Docs          string // Link to docs
}

func (e *EnhancedError) Error() string {
    var b strings.Builder
    
    b.WriteString(fmt.Sprintf("Error: %s\n", e.Message))
    
    if len(e.Context) > 0 {
        b.WriteString("\nContext:\n")
        for k, v := range e.Context {
            b.WriteString(fmt.Sprintf("  %s: %v\n", k, v))
        }
    }
    
    if len(e.Causes) > 0 {
        b.WriteString("\nPossible causes:\n")
        for i, cause := range e.Causes {
            b.WriteString(fmt.Sprintf("  %d. %s\n", i+1, cause))
        }
    }
    
    if len(e.Solutions) > 0 {
        b.WriteString("\nSolutions:\n")
        for _, sol := range e.Solutions {
            b.WriteString(fmt.Sprintf("  - %s\n", sol))
        }
    }
    
    if e.ConfigHint != nil {
        b.WriteString(fmt.Sprintf("\nConfiguration:\n"))
        b.WriteString(fmt.Sprintf("  Current: %s=%s\n", e.ConfigHint.Key, e.ConfigHint.CurrentValue))
        b.WriteString(fmt.Sprintf("  Suggested: %s=%s\n", e.ConfigHint.Key, e.ConfigHint.SuggestedValue))
        b.WriteString(fmt.Sprintf("  Docs: %s\n", e.ConfigHint.Docs))
    }
    
    return b.String()
}
```

### Error Enhancement Examples

**Queue Full Error:**
```go
func newQueueFullError(producer *Producer) error {
    config := producer.GetConfig()
    queueSize := config["queue.buffering.max.messages"]
    
    return &EnhancedError{
        Code:    ErrQueueFull,
        Message: fmt.Sprintf("Producer queue full (%s messages)", queueSize),
        Context: map[string]interface{}{
            "client_type": "producer",
            "client_name": producer.String(),
            "queue_size":  queueSize,
        },
        Causes: []string{
            "Producing faster than broker can accept",
            "Network issues to broker",
            "Broker overloaded",
        },
        Solutions: []string{
            "Increase queue size: queue.buffering.max.messages",
            "Add backpressure handling (check queue before produce)",
            "Reduce produce rate",
            "Check broker health",
        },
        ConfigHint: &ConfigHint{
            Key:            "queue.buffering.max.messages",
            CurrentValue:   queueSize,
            SuggestedValue: fmt.Sprintf("%d", queueSize*2),
            Docs:           "https://docs.confluent.io/...",
        },
    }
}
```

**Authentication Error:**
```go
func newAuthError(broker string, mechanism string) error {
    return &EnhancedError{
        Code:    ErrAuthentication,
        Message: fmt.Sprintf("Authentication failed to broker %s", broker),
        Context: map[string]interface{}{
            "broker":         broker,
            "sasl_mechanism": mechanism,
        },
        Causes: []string{
            "Incorrect username or password",
            "SASL mechanism not supported by broker",
            "SSL/TLS configuration mismatch",
        },
        Solutions: []string{
            "Verify credentials in configuration",
            "Check broker supports SASL mechanism: " + mechanism,
            "Review SSL/TLS settings",
            "Check broker logs for details",
        },
        ConfigHint: &ConfigHint{
            Key:  "sasl.mechanism",
            CurrentValue: mechanism,
            SuggestedValue: "Check broker supports: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI",
            Docs: "https://docs.confluent.io/...",
        },
    }
}
```

## Implementation Plan

**Week 1: Top 20 Errors**
- Day 1-2: Audit most common errors (GitHub issues, support tickets)
- Day 3-4: Implement EnhancedError type
- Day 5: Convert top 10 errors

**Week 2: Remaining Errors & Testing**
- Day 1-2: Convert remaining 10 errors
- Day 3: Integration tests
- Day 4: Update examples with error handling
- Day 5: Documentation

## Success Criteria

- ✅ Top 20 errors have enhanced messages
- ✅ Support questions decrease (track via issues)
- ✅ Users report improved error clarity

**Effort:** 1 week
**Impact:** High (user experience)
