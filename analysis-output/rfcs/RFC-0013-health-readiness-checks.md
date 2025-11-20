# RFC-0013: Health Check & Readiness Endpoints

**Status:** Draft 📝
**Author:** Technical Analysis Team
**Created:** 2025-11-20
**Priority:** P0 (Critical - Kubernetes Requirement)

## Summary

Add standard health check and readiness probe APIs to enable proper Kubernetes liveness and readiness probes, faster incident detection, and better operational visibility.

## Motivation

### Current State

No built-in health check API:

```go
// Users must implement their own health checks
func healthHandler(w http.ResponseWriter, r *http.Request) {
    // How do I check if producer is healthy?
    // How do I know if consumer is connected?
    // Is broker reachable?

    // Currently: No standard way!
    w.WriteHeader(http.StatusOK)
}
```

### Problems

1. **No Kubernetes Probes**: Can't implement proper liveness/readiness
2. **Delayed Failure Detection**: Unhealthy pods serve traffic
3. **Manual Implementation**: Every user reinvents health checks
4. **Inconsistent**: Different apps have different health semantics
5. **Hidden Failures**: Producer queue full but app reports "healthy"

### User Impact

**Production Issues:**
- Unhealthy pods receive traffic (service degradation)
- Kubernetes doesn't restart failed pods (manual intervention)
- Load balancer routes to disconnected consumers
- No early warning before complete failure

## Detailed Design

### Health Status Types

```go
package kafka

// HealthStatus represents overall health
type HealthStatus string

const (
    HealthStatusHealthy   HealthStatus = "healthy"   // Fully operational
    HealthStatusDegraded  HealthStatus = "degraded"  // Partially working
    HealthStatusUnhealthy HealthStatus = "unhealthy" // Not operational
)

// HealthCheck represents a single health check result
type HealthCheck struct {
    Name      string                 // Check name (e.g., "broker_connection")
    Status    HealthStatus           // Health status
    Message   string                 // Human-readable message
    Timestamp time.Time              // When check was performed
    Details   map[string]interface{} // Additional details
}

// HealthReport aggregates all health checks
type HealthReport struct {
    Status  HealthStatus   // Overall status
    Checks  []HealthCheck  // Individual checks
    Uptime  time.Duration  // Time since start
}
```

### Producer Health API

```go
// Producer health checks
func (p *Producer) HealthCheck() HealthReport {
    report := HealthReport{
        Status: HealthStatusHealthy,
        Checks: []HealthCheck{},
        Uptime: time.Since(p.startTime),
    }

    // Check 1: Broker connectivity
    brokerCheck := p.checkBrokerConnectivity()
    report.Checks = append(report.Checks, brokerCheck)

    // Check 2: Queue capacity
    queueCheck := p.checkQueueCapacity()
    report.Checks = append(report.Checks, queueCheck)

    // Check 3: Recent errors
    errorCheck := p.checkRecentErrors()
    report.Checks = append(report.Checks, errorCheck)

    // Aggregate status (worst of all checks)
    for _, check := range report.Checks {
        if check.Status == HealthStatusUnhealthy {
            report.Status = HealthStatusUnhealthy
            break
        } else if check.Status == HealthStatusDegraded {
            report.Status = HealthStatusDegraded
        }
    }

    return report
}

// Check if connected to at least one broker
func (p *Producer) checkBrokerConnectivity() HealthCheck {
    metadata, err := p.GetMetadata(nil, false, 1000)
    if err != nil {
        return HealthCheck{
            Name:    "broker_connection",
            Status:  HealthStatusUnhealthy,
            Message: fmt.Sprintf("No broker connection: %v", err),
        }
    }

    connectedBrokers := 0
    for _, broker := range metadata.Brokers {
        if broker.Connected {
            connectedBrokers++
        }
    }

    if connectedBrokers == 0 {
        return HealthCheck{
            Name:    "broker_connection",
            Status:  HealthStatusUnhealthy,
            Message: "No brokers connected",
        }
    }

    return HealthCheck{
        Name:    "broker_connection",
        Status:  HealthStatusHealthy,
        Message: fmt.Sprintf("%d brokers connected", connectedBrokers),
        Details: map[string]interface{}{
            "connected_brokers": connectedBrokers,
            "total_brokers":     len(metadata.Brokers),
        },
    }
}

// Check queue capacity (degraded if >80% full)
func (p *Producer) checkQueueCapacity() HealthCheck {
    queueSize := p.Len()
    queueCapacity := 100000 // From config
    utilization := float64(queueSize) / float64(queueCapacity) * 100

    if utilization > 95 {
        return HealthCheck{
            Name:    "queue_capacity",
            Status:  HealthStatusUnhealthy,
            Message: fmt.Sprintf("Queue critically full: %.1f%%", utilization),
            Details: map[string]interface{}{
                "queue_size":     queueSize,
                "queue_capacity": queueCapacity,
                "utilization":    utilization,
            },
        }
    } else if utilization > 80 {
        return HealthCheck{
            Name:    "queue_capacity",
            Status:  HealthStatusDegraded,
            Message: fmt.Sprintf("Queue filling up: %.1f%%", utilization),
        }
    }

    return HealthCheck{
        Name:    "queue_capacity",
        Status:  HealthStatusHealthy,
        Message: fmt.Sprintf("Queue utilization: %.1f%%", utilization),
    }
}
```

### Consumer Health API

```go
// Consumer health checks
func (c *Consumer) HealthCheck() HealthReport {
    report := HealthReport{
        Status: HealthStatusHealthy,
        Checks: []HealthCheck{},
        Uptime: time.Since(c.startTime),
    }

    // Check 1: Broker connectivity
    brokerCheck := c.checkBrokerConnectivity()
    report.Checks = append(report.Checks, brokerCheck)

    // Check 2: Partition assignment
    assignmentCheck := c.checkPartitionAssignment()
    report.Checks = append(report.Checks, assignmentCheck)

    // Check 3: Consumer lag
    lagCheck := c.checkConsumerLag()
    report.Checks = append(report.Checks, lagCheck)

    // Aggregate status
    for _, check := range report.Checks {
        if check.Status == HealthStatusUnhealthy {
            report.Status = HealthStatusUnhealthy
            break
        } else if check.Status == HealthStatusDegraded {
            report.Status = HealthStatusDegraded
        }
    }

    return report
}

// Check if partitions are assigned
func (c *Consumer) checkPartitionAssignment() HealthCheck {
    assignment, err := c.Assignment()
    if err != nil {
        return HealthCheck{
            Name:    "partition_assignment",
            Status:  HealthStatusUnhealthy,
            Message: fmt.Sprintf("Cannot get assignment: %v", err),
        }
    }

    if len(assignment) == 0 {
        return HealthCheck{
            Name:    "partition_assignment",
            Status:  HealthStatusDegraded,
            Message: "No partitions assigned (may be rebalancing)",
        }
    }

    return HealthCheck{
        Name:    "partition_assignment",
        Status:  HealthStatusHealthy,
        Message: fmt.Sprintf("%d partitions assigned", len(assignment)),
        Details: map[string]interface{}{
            "partition_count": len(assignment),
        },
    }
}
```

### Kubernetes Integration

```go
// HTTP handler for Kubernetes probes
package main

import (
    "encoding/json"
    "net/http"
    "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Liveness probe - is the application running?
func livenessHandler(producer *kafka.Producer) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        // Liveness: just check if producer isn't panicking
        if producer.IsClosed() {
            w.WriteHeader(http.StatusServiceUnavailable)
            return
        }
        w.WriteHeader(http.StatusOK)
    }
}

// Readiness probe - is the application ready to serve traffic?
func readinessHandler(producer *kafka.Producer) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        report := producer.HealthCheck()

        w.Header().Set("Content-Type", "application/json")

        switch report.Status {
        case kafka.HealthStatusHealthy:
            w.WriteHeader(http.StatusOK)
        case kafka.HealthStatusDegraded:
            w.WriteHeader(http.StatusOK) // Still serve traffic but warn
        case kafka.HealthStatusUnhealthy:
            w.WriteHeader(http.StatusServiceUnavailable)
        }

        json.NewEncoder(w).Encode(report)
    }
}

func main() {
    producer, _ := kafka.NewProducer(&kafka.ConfigMap{...})

    http.HandleFunc("/healthz", livenessHandler(producer))
    http.HandleFunc("/readyz", readinessHandler(producer))
    http.ListenAndServe(":8080", nil)
}
```

### Kubernetes Deployment YAML

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-producer-app
spec:
  template:
    spec:
      containers:
      - name: app
        image: myapp:latest
        ports:
        - containerPort: 8080

        # Liveness probe - restart if unhealthy
        livenessProbe:
          httpGet:
            path: /healthz
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
          timeoutSeconds: 5
          failureThreshold: 3

        # Readiness probe - remove from service if not ready
        readinessProbe:
          httpGet:
            path: /readyz
            port: 8080
          initialDelaySeconds: 10
          periodSeconds: 5
          timeoutSeconds: 3
          failureThreshold: 2
```

## Implementation Plan

**Week 1: Core Health API**
- Implement HealthStatus, HealthCheck, HealthReport types
- Add Producer.HealthCheck() with broker connectivity check
- Add Consumer.HealthCheck() with assignment check

**Week 2: Advanced Checks**
- Queue capacity checks
- Consumer lag checks
- Recent error tracking
- Degraded state detection

**Week 3: Documentation & Examples**
- Kubernetes integration example
- Health check best practices
- Grafana dashboard for health metrics

## Benefits

1. **Kubernetes Native**: Proper liveness/readiness probes
2. **Faster Detection**: Know about issues before complete failure
3. **Self-Healing**: Kubernetes restarts unhealthy pods
4. **Load Balancing**: Don't route to unhealthy instances
5. **Operational Visibility**: Clear health status at a glance

## Success Criteria

- ✅ Producer/Consumer expose HealthCheck() API
- ✅ Kubernetes example with probes
- ✅ Health status accurately reflects reality
- ✅ Documentation on health check configuration

**Effort:** 2 weeks
**Impact:** Critical (Kubernetes requirement, operational necessity)
