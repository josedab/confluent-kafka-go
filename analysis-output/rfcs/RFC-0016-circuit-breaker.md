# RFC-0016: Circuit Breaker for Broker Failures

**Status:** Draft 📝
**Author:** Technical Analysis Team
**Created:** 2025-11-20
**Priority:** P1 (Strategic - Reliability)

## Summary

Implement circuit breaker pattern to gracefully handle repeated broker failures, prevent cascading failures, and enable faster recovery during outages.

## Motivation

### Current State

No circuit breaking - keeps retrying forever:

```go
// If broker is down, keeps trying indefinitely
for {
    err := producer.Produce(msg, nil)
    if err != nil {
        // Retries forever, consuming resources
    }
}
```

### Problems

1. **Resource Exhaustion**: Continuous retries consume CPU/memory
2. **Cascading Failures**: Slow broker affects entire application
3. **No Fast Fail**: Can't detect "broker is down, stop trying"
4. **Hidden Degradation**: App appears slow instead of failing fast

## Detailed Design

### Circuit Breaker States

```go
type CircuitState string

const (
    StateClosed     CircuitState = "closed"      // Normal operation
    StateOpen       CircuitState = "open"        // Failing, reject requests
    StateHalfOpen   CircuitState = "half_open"   // Testing recovery
)

type CircuitBreaker struct {
    state         CircuitState
    failureCount  int
    successCount  int
    threshold     int           // Failures before opening
    timeout       time.Duration // Time in open state
    lastFailure   time.Time
}

// Check if request should proceed
func (cb *CircuitBreaker) Allow() bool {
    switch cb.state {
    case StateClosed:
        return true // Normal operation
    case StateOpen:
        if time.Since(cb.lastFailure) > cb.timeout {
            cb.state = StateHalfOpen
            return true // Try one request
        }
        return false // Still in cooldown
    case StateHalfOpen:
        return true // Testing recovery
    }
    return false
}

// Record success
func (cb *CircuitBreaker) RecordSuccess() {
    if cb.state == StateHalfOpen {
        cb.successCount++
        if cb.successCount >= 3 {
            cb.state = StateClosed
            cb.failureCount = 0
        }
    }
}

// Record failure
func (cb *CircuitBreaker) RecordFailure() {
    cb.lastFailure = time.Now()
    cb.failureCount++

    if cb.failureCount >= cb.threshold {
        cb.state = StateOpen
    }
}
```

### Producer with Circuit Breaker

```go
producer, _ := kafka.NewProducer(&kafka.ConfigMap{
    "bootstrap.servers": "localhost:9092",
    "go.circuit.breaker.enable": true,
    "go.circuit.breaker.threshold": 10,      // Open after 10 failures
    "go.circuit.breaker.timeout": "30s",     // Wait 30s before retry
})

// Produce with circuit breaker
err := producer.Produce(msg, nil)
if err == kafka.ErrCircuitOpen {
    // Circuit is open, fail fast
    log.Warn("Circuit breaker open, skipping produce")
    return err
}
```

## Benefits

1. **Fast Failure**: Detect broker issues quickly
2. **Resource Protection**: Stop wasting resources on failed broker
3. **Graceful Degradation**: Application stays responsive
4. **Automatic Recovery**: Tries again after timeout

**Effort:** 2-3 weeks
**Impact:** High (prevents cascading failures)
