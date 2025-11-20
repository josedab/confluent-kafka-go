# RFC-0014: Graceful Shutdown Improvements

**Status:** Draft 📝
**Author:** Technical Analysis Team
**Created:** 2025-11-20
**Priority:** P0 (Critical - Reliability)

## Summary

Improve graceful shutdown mechanisms to ensure zero message loss during deployments, proper resource cleanup, and clean termination with configurable timeouts.

## Motivation

### Current State

Current shutdown is basic:

```go
producer.Close() // Blocks until queue drains (no timeout)
consumer.Close() // Closes immediately (may lose in-flight messages)
```

### Problems

1. **No Timeout Control**: `Close()` may block forever
2. **Message Loss Risk**: Consumer closes before committing offsets
3. **Unclear Semantics**: What does "graceful" actually mean?
4. **No Progress Visibility**: Can't monitor shutdown progress
5. **Resource Leaks**: Goroutines may not terminate

### User Impact

**Production Issues:**
- Deployments block waiting for producer drain
- Messages lost during consumer shutdown
- Kubernetes kills pod before clean shutdown (SIGKILL)
- No way to implement graceful draining

## Detailed Design

### Producer Graceful Shutdown

```go
// Close with context for timeout control
func (p *Producer) CloseWithContext(ctx context.Context) error {
    // Step 1: Stop accepting new messages
    p.markClosing()

    // Step 2: Drain queue with timeout
    remaining := p.Len()
    ticker := time.NewTicker(100 * time.Millisecond)
    defer ticker.Stop()

    for remaining > 0 {
        select {
        case <-ctx.Done():
            return &ShutdownError{
                Phase:           "draining",
                RemainingMessages: remaining,
                Err:             ctx.Err(),
            }
        case <-ticker.C:
            remaining = p.Len()
        }
    }

    // Step 3: Flush remaining messages
    if err := p.FlushWithContext(ctx); err != nil {
        return err
    }

    // Step 4: Close handle
    p.handle.Close()

    return nil
}

// Flush with context support
func (p *Producer) FlushWithContext(ctx context.Context) error {
    remaining := p.Len()

    for remaining > 0 {
        select {
        case <-ctx.Done():
            return &FlushError{
                RemainingMessages: remaining,
                Err:             ctx.Err(),
            }
        default:
            // Call Flush with short timeout, check context repeatedly
            p.Flush(100)
            remaining = p.Len()
        }
    }

    return nil
}

// Shutdown errors
type ShutdownError struct {
    Phase             string // Which phase failed
    RemainingMessages int    // Messages not delivered
    Err               error  // Underlying error
}

func (e *ShutdownError) Error() string {
    return fmt.Sprintf("shutdown failed during %s: %d messages remaining: %v",
        e.Phase, e.RemainingMessages, e.Err)
}
```

### Consumer Graceful Shutdown

```go
// Close with context and commit
func (c *Consumer) CloseWithContext(ctx context.Context) error {
    // Step 1: Stop fetching new messages
    c.markClosing()

    // Step 2: Process in-flight messages (up to context deadline)
    if err := c.drainInFlight(ctx); err != nil {
        return err
    }

    // Step 3: Commit final offsets
    if err := c.commitSync(ctx); err != nil {
        return err
    }

    // Step 4: Leave group gracefully
    if err := c.Unsubscribe(); err != nil {
        return err
    }

    // Step 5: Close handle
    c.handle.Close()

    return nil
}

// Drain in-flight messages
func (c *Consumer) drainInFlight(ctx context.Context) error {
    // Allow application to process messages being handled
    ticker := time.NewTicker(100 * time.Millisecond)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        case <-ticker.C:
            if c.inFlightCount() == 0 {
                return nil
            }
        }
    }
}

// Commit with context
func (c *Consumer) commitSync(ctx context.Context) error {
    done := make(chan error, 1)

    go func() {
        _, err := c.CommitOffsets(nil)
        done <- err
    }()

    select {
    case err := <-done:
        return err
    case <-ctx.Done():
        return ctx.Err()
    }
}
```

### Graceful Shutdown Manager

```go
package kafka

// ShutdownManager coordinates graceful shutdown of multiple clients
type ShutdownManager struct {
    producers []*Producer
    consumers []*Consumer
    timeout   time.Duration
}

// NewShutdownManager creates a shutdown coordinator
func NewShutdownManager(timeout time.Duration) *ShutdownManager {
    return &ShutdownManager{
        timeout: timeout,
    }
}

// Register adds clients to manage
func (m *ShutdownManager) RegisterProducer(p *Producer) {
    m.producers = append(m.producers, p)
}

func (m *ShutdownManager) RegisterConsumer(c *Consumer) {
    m.consumers = append(m.consumers, c)
}

// Shutdown performs coordinated graceful shutdown
func (m *ShutdownManager) Shutdown(ctx context.Context) error {
    ctx, cancel := context.WithTimeout(ctx, m.timeout)
    defer cancel()

    var wg sync.WaitGroup
    errors := make(chan error, len(m.producers)+len(m.consumers))

    // Step 1: Stop all producers first (drain queues)
    for _, p := range m.producers {
        wg.Add(1)
        go func(producer *Producer) {
            defer wg.Done()
            if err := producer.CloseWithContext(ctx); err != nil {
                errors <- fmt.Errorf("producer shutdown: %w", err)
            }
        }(p)
    }

    wg.Wait()

    // Step 2: Stop all consumers (commit offsets)
    for _, c := range m.consumers {
        wg.Add(1)
        go func(consumer *Consumer) {
            defer wg.Done()
            if err := consumer.CloseWithContext(ctx); err != nil {
                errors <- fmt.Errorf("consumer shutdown: %w", err)
            }
        }(c)
    }

    wg.Wait()
    close(errors)

    // Collect errors
    var shutdownErrors []error
    for err := range errors {
        shutdownErrors = append(shutdownErrors, err)
    }

    if len(shutdownErrors) > 0 {
        return fmt.Errorf("shutdown errors: %v", shutdownErrors)
    }

    return nil
}
```

### Signal Handling Example

```go
package main

import (
    "context"
    "os"
    "os/signal"
    "syscall"
    "time"
    "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
    producer, _ := kafka.NewProducer(&kafka.ConfigMap{...})
    consumer, _ := kafka.NewConsumer(&kafka.ConfigMap{...})

    // Create shutdown manager
    shutdown := kafka.NewShutdownManager(30 * time.Second)
    shutdown.RegisterProducer(producer)
    shutdown.RegisterConsumer(consumer)

    // Setup signal handling
    sigterm := make(chan os.Signal, 1)
    signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

    // Application logic
    go produceMessages(producer)
    go consumeMessages(consumer)

    // Wait for termination signal
    <-sigterm
    log.Println("Shutting down gracefully...")

    // Graceful shutdown with timeout
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    if err := shutdown.Shutdown(ctx); err != nil {
        log.Printf("Shutdown error: %v", err)
        os.Exit(1)
    }

    log.Println("Shutdown complete")
}
```

### Kubernetes PreStop Hook

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-app
spec:
  template:
    spec:
      containers:
      - name: app
        image: myapp:latest
        lifecycle:
          preStop:
            exec:
              # Call HTTP endpoint that triggers graceful shutdown
              command: ["/bin/sh", "-c", "curl -X POST localhost:8080/shutdown && sleep 5"]

        # Give enough time for graceful shutdown
        terminationGracePeriodSeconds: 35
```

## Implementation Plan

**Week 1: Core Shutdown API**
- Implement CloseWithContext() for Producer
- Implement CloseWithContext() for Consumer
- Error types (ShutdownError, FlushError)

**Week 2: Shutdown Manager**
- Implement ShutdownManager
- Coordinate multiple clients
- Progress tracking

**Week 3: Documentation & Examples**
- Signal handling example
- Kubernetes integration
- Best practices guide

## Benefits

1. **Zero Message Loss**: Proper draining ensures all messages delivered
2. **Controlled Shutdown**: Timeout prevents infinite blocking
3. **Kubernetes Ready**: Works with preStop hooks and termination grace period
4. **Progress Visibility**: Can monitor shutdown status
5. **Clean Termination**: All resources properly released

## Success Criteria

- ✅ CloseWithContext() respects timeouts
- ✅ Producer drains queue before closing
- ✅ Consumer commits offsets before closing
- ✅ ShutdownManager coordinates multiple clients
- ✅ Examples show signal handling

**Effort:** 2-3 weeks
**Impact:** Critical (zero message loss, proper cleanup)
