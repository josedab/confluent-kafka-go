# RFC-0003: Implement Metrics Exporter (Prometheus)

**Status:** Draft 📝
**Author:** Technical Analysis Team
**Created:** 2025-11-16
**Updated:** 2025-11-16
**Priority:** P1 (Strategic)

---

## Summary

Implement a Prometheus-compatible metrics exporter that exposes production-ready metrics for Producer, Consumer, and AdminClient. This provides standardized observability for throughput, latency, errors, and resource utilization without requiring custom stats event parsing.

---

## Motivation

### Current State

confluent-kafka-go provides statistics via `StatsEvent`:

```go
// kafka/event.go
type StatsEvent struct {
    Stats string // JSON blob from librdkafka
}

// Usage
for e := range producer.Events() {
    switch ev := e.(type) {
    case *kafka.StatsEvent:
        // User must parse JSON and extract metrics manually
        stats := parseJSON(ev.Stats)
        // Now what? Export to Prometheus? InfluxDB? Custom dashboard?
    }
}
```

**Problems:**
1. **Manual Parsing**: Users must parse complex JSON structure
2. **No Standard Format**: Everyone implements custom exporters
3. **Steep Learning Curve**: 200+ metrics in stats JSON, which ones matter?
4. **No Integration**: Doesn't work with standard monitoring (Prometheus, Grafana)
5. **Polling Required**: Stats events need manual collection

### User Pain Points

**Current workflow:**
1. Enable stats: `"statistics.interval.ms": 5000`
2. Receive StatsEvent with 200+ metrics in JSON
3. Parse JSON manually
4. Figure out which metrics are important
5. Implement custom Prometheus exporter
6. Create Grafana dashboards from scratch

**Desired workflow:**
1. Import `kafka/metrics` package
2. Call `metrics.RegisterPrometheusExporter(producer)`
3. Expose `/metrics` HTTP endpoint
4. Use pre-built Grafana dashboards

---

## Detailed Design

### Architecture

```
┌─────────────────────────────────────────────────────┐
│  Producer/Consumer/AdminClient                      │
└────────────────┬────────────────────────────────────┘
                 │ StatsEvent (JSON)
                 ↓
┌─────────────────────────────────────────────────────┐
│  Metrics Collector (new)                            │
│  - Parses StatsEvent JSON                           │
│  - Extracts key metrics                             │
│  - Updates Prometheus gauges/counters               │
└────────────────┬────────────────────────────────────┘
                 │
                 ↓
┌─────────────────────────────────────────────────────┐
│  Prometheus Registry                                │
│  - Standard prometheus/client_golang                │
└────────────────┬────────────────────────────────────┘
                 │
                 ↓
┌─────────────────────────────────────────────────────┐
│  HTTP /metrics Endpoint                             │
│  - Scraped by Prometheus server                     │
└─────────────────────────────────────────────────────┘
```

---

### Implementation

#### 1. New Package: `kafka/metrics`

**File: `kafka/metrics/metrics.go`**

```go
package metrics

import (
    "encoding/json"
    "github.com/confluentinc/confluent-kafka-go/v2/kafka"
    "github.com/prometheus/client_golang/prometheus"
)

// MetricsExporter collects metrics from Kafka clients and exports to Prometheus.
type MetricsExporter struct {
    registry *prometheus.Registry

    // Producer metrics
    producerMessagesProduced    *prometheus.CounterVec
    producerBytesProduced       *prometheus.CounterVec
    producerErrors              *prometheus.CounterVec
    producerLatencyP50          *prometheus.GaugeVec
    producerLatencyP95          *prometheus.GaugeVec
    producerLatencyP99          *prometheus.GaugeVec
    producerQueueSize           *prometheus.GaugeVec
    producerQueueCapacity       *prometheus.GaugeVec

    // Consumer metrics
    consumerMessagesConsumed    *prometheus.CounterVec
    consumerBytesConsumed       *prometheus.CounterVec
    consumerLag                 *prometheus.GaugeVec
    consumerErrors              *prometheus.CounterVec
    consumerRebalances          *prometheus.CounterVec
    consumerFetchLatency        *prometheus.GaugeVec

    // Connection metrics
    connectionCount             *prometheus.GaugeVec
    connectionErrors            *prometheus.CounterVec

    // System metrics
    memoryUsage                 *prometheus.GaugeVec
    goroutines                  *prometheus.GaugeVec
}

// NewMetricsExporter creates a new Prometheus metrics exporter.
func NewMetricsExporter(namespace string) *MetricsExporter {
    registry := prometheus.NewRegistry()

    return &MetricsExporter{
        registry: registry,
        producerMessagesProduced: prometheus.NewCounterVec(
            prometheus.CounterOpts{
                Namespace: namespace,
                Subsystem: "producer",
                Name:      "messages_total",
                Help:      "Total number of messages produced",
            },
            []string{"client_id", "topic"},
        ),
        producerBytesProduced: prometheus.NewCounterVec(
            prometheus.CounterOpts{
                Namespace: namespace,
                Subsystem: "producer",
                Name:      "bytes_total",
                Help:      "Total bytes produced",
            },
            []string{"client_id", "topic"},
        ),
        producerLatencyP99: prometheus.NewGaugeVec(
            prometheus.GaugeOpts{
                Namespace: namespace,
                Subsystem: "producer",
                Name:      "latency_p99_milliseconds",
                Help:      "99th percentile produce latency in milliseconds",
            },
            []string{"client_id"},
        ),
        consumerLag: prometheus.NewGaugeVec(
            prometheus.GaugeOpts{
                Namespace: namespace,
                Subsystem: "consumer",
                Name:      "lag_messages",
                Help:      "Consumer lag (messages behind high water mark)",
            },
            []string{"client_id", "topic", "partition"},
        ),
        // ... register all metrics ...
    }
}

// RegisterProducer starts collecting metrics from a Producer.
func (e *MetricsExporter) RegisterProducer(producer *kafka.Producer) error {
    // Enable statistics
    producer.SetStatisticsInterval(5000) // 5 seconds

    // Start goroutine to collect stats
    go e.collectProducerStats(producer)

    return nil
}

// RegisterConsumer starts collecting metrics from a Consumer.
func (e *MetricsExporter) RegisterConsumer(consumer *kafka.Consumer) error {
    consumer.SetStatisticsInterval(5000)
    go e.collectConsumerStats(consumer)
    return nil
}

// collectProducerStats parses StatsEvent and updates Prometheus metrics.
func (e *MetricsExporter) collectProducerStats(producer *kafka.Producer) {
    for event := range producer.Events() {
        statsEvent, ok := event.(*kafka.StatsEvent)
        if !ok {
            continue
        }

        var stats ProducerStats
        if err := json.Unmarshal([]byte(statsEvent.Stats), &stats); err != nil {
            continue
        }

        clientID := stats.Name

        // Update counters
        for topic, topicStats := range stats.Topics {
            e.producerMessagesProduced.WithLabelValues(clientID, topic).
                Add(float64(topicStats.Messages))

            e.producerBytesProduced.WithLabelValues(clientID, topic).
                Add(float64(topicStats.Bytes))
        }

        // Update gauges
        e.producerLatencyP99.WithLabelValues(clientID).
            Set(float64(stats.InternalLatency.P99) / 1000.0) // Convert µs to ms

        e.producerQueueSize.WithLabelValues(clientID).
            Set(float64(stats.MessageCount))
    }
}

// Registry returns the Prometheus registry for HTTP handler.
func (e *MetricsExporter) Registry() *prometheus.Registry {
    return e.registry
}
```

---

#### 2. Stats Parsing Structures

**File: `kafka/metrics/types.go`**

```go
package metrics

// ProducerStats represents parsed librdkafka producer statistics.
// Based on: https://github.com/confluentinc/librdkafka/blob/master/STATISTICS.md
type ProducerStats struct {
    Name            string                 `json:"name"`
    Type            string                 `json:"type"`
    Timestamp       int64                  `json:"ts"`
    Time            int64                  `json:"time"`
    MessageCount    int                    `json:"msg_cnt"`
    MessageSize     int                    `json:"msg_size"`
    MessageMax      int                    `json:"msg_max"`
    Topics          map[string]TopicStats  `json:"topics"`
    Brokers         map[string]BrokerStats `json:"brokers"`
    InternalLatency LatencyStats           `json:"int_latency"`
    OutboundQueue   int                    `json:"outbuf_cnt"`
    WaitingReplies  int                    `json:"waitresp_cnt"`
}

type TopicStats struct {
    Topic      string                    `json:"topic"`
    Messages   int64                     `json:"txmsgs"`
    Bytes      int64                     `json:"txbytes"`
    Partitions map[string]PartitionStats `json:"partitions"`
}

type PartitionStats struct {
    Partition       int   `json:"partition"`
    Messages        int64 `json:"txmsgs"`
    Bytes           int64 `json:"txbytes"`
    MessagesInFlight int   `json:"msgs_inflight"`
}

type BrokerStats struct {
    Name            string `json:"name"`
    State           string `json:"state"` // "UP", "DOWN", etc.
    StateAge        int64  `json:"stateage"`
    OutboundQueue   int    `json:"outbuf_cnt"`
    WaitingReplies  int    `json:"waitresp_cnt"`
    Requests        int64  `json:"tx"`
    Bytes           int64  `json:"txbytes"`
    Responses       int64  `json:"rx"`
    ResponseBytes   int64  `json:"rxbytes"`
    RequestLatency  LatencyStats `json:"rtt"`
}

type LatencyStats struct {
    Min    int `json:"min"`    // Microseconds
    Max    int `json:"max"`
    Avg    int `json:"avg"`
    Sum    int `json:"sum"`
    Count  int `json:"cnt"`
    StdDev int `json:"stddev"`
    P50    int `json:"p50"`
    P75    int `json:"p75"`
    P90    int `json:"p90"`
    P95    int `json:"p95"`
    P99    int `json:"p99"`
    P9999  int `json:"p99_99"`
}

// ConsumerStats represents parsed librdkafka consumer statistics.
type ConsumerStats struct {
    Name            string                 `json:"name"`
    Type            string                 `json:"type"`
    ConsumerGroup   string                 `json:"rebalance_group_id"`
    Topics          map[string]TopicStats  `json:"topics"`
    ConsumerLag     int64                  `json:"consumer_lag"`
}
```

---

#### 3. HTTP Endpoint Setup

**File: `kafka/metrics/http.go`**

```go
package metrics

import (
    "net/http"
    "github.com/prometheus/client_golang/prometheus/promhttp"
)

// HTTPHandler returns an http.Handler for serving Prometheus metrics.
func (e *MetricsExporter) HTTPHandler() http.Handler {
    return promhttp.HandlerFor(e.registry, promhttp.HandlerOpts{
        EnableOpenMetrics: true,
    })
}

// StartHTTPServer starts an HTTP server on the given address.
// Blocks until server stops.
func (e *MetricsExporter) StartHTTPServer(addr string) error {
    http.Handle("/metrics", e.HTTPHandler())
    return http.ListenAndServe(addr, nil)
}
```

---

### Example Usage

#### Basic Producer Metrics

```go
package main

import (
    "net/http"
    "github.com/confluentinc/confluent-kafka-go/v2/kafka"
    "github.com/confluentinc/confluent-kafka-go/v2/kafka/metrics"
)

func main() {
    // Create producer
    producer, err := kafka.NewProducer(&kafka.ConfigMap{
        "bootstrap.servers": "localhost:9092",
    })
    if err != nil {
        panic(err)
    }
    defer producer.Close()

    // Setup metrics exporter
    exporter := metrics.NewMetricsExporter("my_app")
    exporter.RegisterProducer(producer)

    // Expose metrics endpoint
    http.Handle("/metrics", exporter.HTTPHandler())
    go http.ListenAndServe(":8080", nil)

    // Produce messages...
    topic := "test"
    for i := 0; i < 1000; i++ {
        producer.Produce(&kafka.Message{
            TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
            Value:          []byte("message"),
        }, nil)
    }

    producer.Flush(15000)
}
```

**Prometheus scrape config:**
```yaml
scrape_configs:
  - job_name: 'kafka_producer'
    static_configs:
      - targets: ['localhost:8080']
```

**Exposed metrics:**
```
# HELP my_app_producer_messages_total Total number of messages produced
# TYPE my_app_producer_messages_total counter
my_app_producer_messages_total{client_id="rdkafka#producer-1",topic="test"} 1000

# HELP my_app_producer_latency_p99_milliseconds 99th percentile produce latency
# TYPE my_app_producer_latency_p99_milliseconds gauge
my_app_producer_latency_p99_milliseconds{client_id="rdkafka#producer-1"} 12.5

# HELP my_app_producer_queue_size Current number of messages in queue
# TYPE my_app_producer_queue_size gauge
my_app_producer_queue_size{client_id="rdkafka#producer-1"} 0
```

---

#### Consumer Metrics with Lag Monitoring

```go
package main

import (
    "net/http"
    "github.com/confluentinc/confluent-kafka-go/v2/kafka"
    "github.com/confluentinc/confluent-kafka-go/v2/kafka/metrics"
)

func main() {
    consumer, _ := kafka.NewConsumer(&kafka.ConfigMap{
        "bootstrap.servers": "localhost:9092",
        "group.id":          "my-group",
    })
    defer consumer.Close()

    exporter := metrics.NewMetricsExporter("my_app")
    exporter.RegisterConsumer(consumer)

    http.Handle("/metrics", exporter.HTTPHandler())
    go http.ListenAndServe(":8080", nil)

    consumer.SubscribeTopics([]string{"test"}, nil)

    for {
        msg, _ := consumer.Poll(100)
        if msg != nil {
            // Process message
        }
    }
}
```

**Consumer lag metric:**
```
# HELP my_app_consumer_lag_messages Consumer lag (messages behind high water mark)
# TYPE my_app_consumer_lag_messages gauge
my_app_consumer_lag_messages{client_id="rdkafka#consumer-1",topic="test",partition="0"} 1234
my_app_consumer_lag_messages{client_id="rdkafka#consumer-1",topic="test",partition="1"} 567
```

---

### Key Metrics Exported

#### Producer Metrics

| Metric | Type | Description | Labels |
|--------|------|-------------|--------|
| `producer_messages_total` | Counter | Total messages produced | client_id, topic |
| `producer_bytes_total` | Counter | Total bytes produced | client_id, topic |
| `producer_errors_total` | Counter | Total produce errors | client_id, topic, error_type |
| `producer_latency_p50_ms` | Gauge | 50th percentile latency | client_id |
| `producer_latency_p95_ms` | Gauge | 95th percentile latency | client_id |
| `producer_latency_p99_ms` | Gauge | 99th percentile latency | client_id |
| `producer_queue_size` | Gauge | Messages in internal queue | client_id |
| `producer_queue_capacity` | Gauge | Max queue capacity | client_id |

#### Consumer Metrics

| Metric | Type | Description | Labels |
|--------|------|-------------|--------|
| `consumer_messages_total` | Counter | Total messages consumed | client_id, topic |
| `consumer_bytes_total` | Counter | Total bytes consumed | client_id, topic |
| `consumer_lag_messages` | Gauge | Consumer lag (messages) | client_id, topic, partition |
| `consumer_errors_total` | Counter | Total consume errors | client_id, error_type |
| `consumer_rebalances_total` | Counter | Total rebalances | client_id |
| `consumer_fetch_latency_ms` | Gauge | Fetch latency | client_id |

#### Connection Metrics

| Metric | Type | Description | Labels |
|--------|------|-------------|--------|
| `broker_connections` | Gauge | Active broker connections | client_id, broker |
| `broker_connection_errors_total` | Counter | Connection errors | client_id, broker |
| `broker_request_latency_p99_ms` | Gauge | Request latency to broker | client_id, broker |

---

## Implementation Plan

### Timeline: 2 Weeks

**Week 1: Core Implementation**

**Day 1-2: Metrics Package Structure**
- [ ] Create `kafka/metrics/` package
- [ ] Define ProducerStats and ConsumerStats types
- [ ] Implement JSON parsing from StatsEvent

**Day 3-4: Prometheus Integration**
- [ ] Add prometheus/client_golang dependency
- [ ] Implement MetricsExporter with all key metrics
- [ ] Add HTTP handler

**Day 5: Testing**
- [ ] Unit tests for stats parsing
- [ ] Integration tests with mock producer/consumer
- [ ] Verify metrics accuracy

---

**Week 2: Documentation & Examples**

**Day 1-2: Examples**
- [ ] Create `examples/prometheus_producer_example/`
- [ ] Create `examples/prometheus_consumer_example/`
- [ ] Create docker-compose with Prometheus + Grafana

**Day 3: Grafana Dashboards**
- [ ] Create producer dashboard JSON
- [ ] Create consumer dashboard JSON
- [ ] Add screenshots to documentation

**Day 4: Documentation**
- [ ] Update README with metrics section
- [ ] Write metrics guide (which metrics to monitor)
- [ ] Add alerting examples

**Day 5: Final Review**
- [ ] Performance testing (overhead measurement)
- [ ] Documentation review
- [ ] Create PR

---

## Backward Compatibility

### ✅ 100% Backward Compatible

**Optional Feature:**
- Metrics exporter is opt-in (separate package)
- Existing code unaffected
- No changes to core kafka package

**No Breaking Changes:**
- StatsEvent API unchanged
- Users can still parse stats JSON manually if desired
- New dependency (prometheus/client_golang) only pulled if metrics package imported

---

## Performance Impact

### Expected Overhead

**Stats Collection:** ~1-5% CPU (librdkafka already collects stats)
**JSON Parsing:** ~100µs per stats interval (5 seconds default)
**Prometheus Update:** ~50µs per metric update

**Total Impact:** Negligible (<1% at 5-second intervals)

**Mitigation:**
- Configurable stats interval (`statistics.interval.ms`)
- Lazy metric registration (only create metrics that are used)
- Efficient JSON parsing (use standard library)

---

## Alternatives Considered

### Alternative 1: Parse Stats JSON in Application Code

**Current approach** (users do this manually)

**Rejected Because:**
- Every user reimplements the same logic
- No standardization
- Error-prone (stats JSON is complex)

---

### Alternative 2: Use InfluxDB Line Protocol

**Approach:** Export metrics in InfluxDB format instead of Prometheus

**Rejected Because:**
- Prometheus is more popular in Cloud Native ecosystem
- Prometheus has better Grafana integration
- Can add InfluxDB exporter later if needed

---

### Alternative 3: Built-in HTTP Server

**Approach:** Start HTTP server automatically

**Rejected Because:**
- Too opinionated (users might have existing HTTP server)
- Port conflict risks
- Better to provide handler, let users control server

---

## Success Criteria

### Functional
- ✅ All key producer/consumer metrics exposed
- ✅ Prometheus scraping works
- ✅ Grafana dashboards provided
- ✅ Accuracy validated against StatsEvent JSON

### Performance
- ✅ Overhead < 1% CPU
- ✅ Memory increase < 10MB per client

### Usability
- ✅ Setup in < 5 lines of code
- ✅ Pre-built Grafana dashboards work out-of-box
- ✅ Documentation includes alerting examples

---

## Grafana Dashboard Example

### Producer Dashboard Panels

1. **Message Throughput** (rate(producer_messages_total[1m]))
2. **Byte Throughput** (rate(producer_bytes_total[1m]))
3. **Latency Percentiles** (p50, p95, p99 on same graph)
4. **Queue Size** (producer_queue_size)
5. **Error Rate** (rate(producer_errors_total[1m]))
6. **Broker Connection Status** (broker_connections)

### Consumer Dashboard Panels

1. **Message Consumption Rate** (rate(consumer_messages_total[1m]))
2. **Consumer Lag** (consumer_lag_messages by partition)
3. **Rebalance Frequency** (rate(consumer_rebalances_total[1h]))
4. **Error Rate** (rate(consumer_errors_total[1m]))

### Alerting Rules Example

```yaml
# prometheus/alerts.yml
groups:
  - name: kafka_producer
    rules:
      - alert: HighProducerLatency
        expr: my_app_producer_latency_p99_milliseconds > 100
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Producer latency high ({{ $value }}ms)"

      - alert: ProducerQueueFull
        expr: my_app_producer_queue_size / my_app_producer_queue_capacity > 0.9
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Producer queue nearly full ({{ $value }}%)"

  - name: kafka_consumer
    rules:
      - alert: HighConsumerLag
        expr: my_app_consumer_lag_messages > 10000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Consumer lag high ({{ $value }} messages)"
```

---

## References

- [librdkafka Statistics](https://github.com/confluentinc/librdkafka/blob/master/STATISTICS.md)
- [Prometheus Best Practices](https://prometheus.io/docs/practices/naming/)
- [Grafana Dashboards](https://grafana.com/grafana/dashboards/)
- [Confluent Metrics](https://docs.confluent.io/platform/current/kafka/monitoring.html)

---

## Conclusion

A Prometheus metrics exporter transforms confluent-kafka-go from "you figure out monitoring" to "monitoring works out-of-the-box." This is critical for production adoption and reduces the barrier to observability.

**Recommended Action:** Approve for implementation in Q2 2026 (2-week sprint).

---

**Status**: 📝 Draft — Awaiting maintainer review
**Next Review**: 2025-12-01
