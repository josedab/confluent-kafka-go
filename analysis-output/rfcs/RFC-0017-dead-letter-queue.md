# RFC-0017: Dead Letter Queue (DLQ) Pattern

**Status:** Draft 📝
**Author:** Technical Analysis Team
**Created:** 2025-11-20
**Priority:** P1 (Strategic - Error Handling)

## Summary

Implement built-in Dead Letter Queue (DLQ) support for handling poison messages with configurable retry policies and automatic DLQ routing.

## Motivation

### Current State

Users implement DLQ manually:

```go
for {
    msg, _ := consumer.ReadMessage(-1)

    err := processMessage(msg)
    if err != nil {
        // Manual DLQ logic (boilerplate in every service)
        retryCount := getRetryCount(msg)
        if retryCount > 3 {
            sendToDLQ(msg) // Users implement this
        } else {
            retry(msg) // And this
        }
    }
}
```

### Problems

1. **Boilerplate**: Every service reinvents DLQ logic
2. **Inconsistent**: Different retry strategies across services
3. **Error Prone**: Easy to lose messages if DLQ fails
4. **No Standards**: No common pattern for retry metadata

## Detailed Design

### DLQ Configuration

```go
consumer, _ := kafka.NewConsumer(&kafka.ConfigMap{
    "bootstrap.servers": "localhost:9092",
    "group.id": "my-group",

    // DLQ configuration
    "go.dlq.enable": true,
    "go.dlq.topic": "my-topic-dlq",
    "go.dlq.max.retries": 3,
    "go.dlq.retry.backoff.ms": "1000,5000,10000", // Exponential backoff
})

// Consumer with DLQ support
func processWithDLQ(consumer *kafka.Consumer, handler func(*kafka.Message) error) {
    for {
        msg, _ := consumer.ReadMessage(-1)

        // Try processing with automatic retry + DLQ
        err := consumer.ProcessWithDLQ(msg, handler)
        if err != nil {
            log.Printf("Message sent to DLQ: %v", err)
        }
    }
}
```

### DLQ Headers

```go
// Automatic headers added to DLQ messages
type DLQHeaders struct {
    OriginalTopic     string    // Where message came from
    FailureReason     string    // Why it failed
    RetryCount        int       // How many retries attempted
    FirstFailureTime  time.Time // When first failure occurred
    LastFailureTime   time.Time // When last retry failed
    ProcessingHistory []string  // Stack trace or error history
}
```

### ProcessWithDLQ Implementation

```go
func (c *Consumer) ProcessWithDLQ(msg *Message, handler func(*Message) error) error {
    retryCount := c.getRetryCount(msg)
    maxRetries := c.config.DLQMaxRetries

    // Try processing
    err := handler(msg)
    if err == nil {
        c.CommitMessage(msg)
        return nil
    }

    // Check if should retry or send to DLQ
    if retryCount >= maxRetries {
        return c.sendToDLQ(msg, err)
    }

    // Retry with backoff
    return c.scheduleRetry(msg, retryCount+1, err)
}

// Send message to DLQ
func (c *Consumer) sendToDLQ(msg *Message, processingErr error) error {
    dlqTopic := c.config.DLQTopic

    dlqMsg := &Message{
        TopicPartition: TopicPartition{Topic: &dlqTopic},
        Key:            msg.Key,
        Value:          msg.Value,
        Headers:        c.addDLQHeaders(msg, processingErr),
    }

    return c.dlqProducer.Produce(dlqMsg, nil)
}
```

## Benefits

1. **No Boilerplate**: Built-in retry + DLQ logic
2. **Consistent**: Standard retry patterns across services
3. **Observability**: Standard DLQ headers for debugging
4. **Configurable**: Flexible retry policies

**Effort:** 3 weeks
**Impact:** Very High (common production requirement)
