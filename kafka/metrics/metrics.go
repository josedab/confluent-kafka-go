/**
 * Copyright 2025 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Package metrics provides Prometheus-compatible metrics exporters for Kafka clients.
//
// Example usage:
//
//	// Create metrics exporter
//	exporter := metrics.NewMetricsExporter("my_app")
//
//	// Register producer
//	exporter.RegisterProducer(producer)
//
//	// Expose HTTP endpoint
//	http.Handle("/metrics", exporter.HTTPHandler())
//	http.ListenAndServe(":9090", nil)
package metrics

import (
	"encoding/json"
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// MetricsExporter collects metrics from Kafka clients and exports to Prometheus.
type MetricsExporter struct {
	registry  *prometheus.Registry
	namespace string
	mu        sync.RWMutex
	clients   map[string]*clientMetrics

	// Producer metrics
	producerMessagesProduced *prometheus.CounterVec
	producerBytesProduced    *prometheus.CounterVec
	producerErrors           *prometheus.CounterVec
	producerLatencyP99       *prometheus.GaugeVec
	producerQueueSize        *prometheus.GaugeVec

	// Consumer metrics
	consumerMessagesConsumed *prometheus.CounterVec
	consumerBytesConsumed    *prometheus.CounterVec
	consumerLag              *prometheus.GaugeVec
	consumerErrors           *prometheus.CounterVec
	consumerRebalances       *prometheus.CounterVec

	// Connection metrics
	connectionCount *prometheus.GaugeVec
}

type clientMetrics struct {
	clientID   string
	clientType string
	stopChan   chan struct{}
}

// NewMetricsExporter creates a new Prometheus metrics exporter.
//
// The namespace parameter sets the Prometheus metric prefix.
// For example, with namespace "myapp", metrics will be named:
//   - myapp_producer_messages_total
//   - myapp_consumer_lag_messages
//
// Example:
//
//	exporter := metrics.NewMetricsExporter("order_service")
func NewMetricsExporter(namespace string) *MetricsExporter {
	registry := prometheus.NewRegistry()

	e := &MetricsExporter{
		registry:  registry,
		namespace: namespace,
		clients:   make(map[string]*clientMetrics),
	}

	// Initialize producer metrics
	e.producerMessagesProduced = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "producer",
			Name:      "messages_total",
			Help:      "Total number of messages produced",
		},
		[]string{"client_id", "topic"},
	)

	e.producerBytesProduced = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "producer",
			Name:      "bytes_total",
			Help:      "Total bytes produced",
		},
		[]string{"client_id"},
	)

	e.producerErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "producer",
			Name:      "errors_total",
			Help:      "Total number of producer errors",
		},
		[]string{"client_id", "error_type"},
	)

	e.producerLatencyP99 = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "producer",
			Name:      "latency_p99_milliseconds",
			Help:      "99th percentile produce latency in milliseconds",
		},
		[]string{"client_id"},
	)

	e.producerQueueSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "producer",
			Name:      "queue_size",
			Help:      "Current number of messages in producer queue",
		},
		[]string{"client_id"},
	)

	// Initialize consumer metrics
	e.consumerMessagesConsumed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "consumer",
			Name:      "messages_total",
			Help:      "Total number of messages consumed",
		},
		[]string{"client_id", "topic", "partition"},
	)

	e.consumerBytesConsumed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "consumer",
			Name:      "bytes_total",
			Help:      "Total bytes consumed",
		},
		[]string{"client_id"},
	)

	e.consumerLag = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "consumer",
			Name:      "lag_messages",
			Help:      "Consumer lag (messages behind high water mark)",
		},
		[]string{"client_id", "topic", "partition"},
	)

	e.consumerErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "consumer",
			Name:      "errors_total",
			Help:      "Total number of consumer errors",
		},
		[]string{"client_id", "error_type"},
	)

	e.consumerRebalances = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "consumer",
			Name:      "rebalances_total",
			Help:      "Total number of consumer group rebalances",
		},
		[]string{"client_id", "group_id"},
	)

	// Initialize connection metrics
	e.connectionCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "connection",
			Name:      "broker_connections",
			Help:      "Number of active broker connections",
		},
		[]string{"client_id"},
	)

	// Register all metrics
	registry.MustRegister(
		e.producerMessagesProduced,
		e.producerBytesProduced,
		e.producerErrors,
		e.producerLatencyP99,
		e.producerQueueSize,
		e.consumerMessagesConsumed,
		e.consumerBytesConsumed,
		e.consumerLag,
		e.consumerErrors,
		e.consumerRebalances,
		e.connectionCount,
	)

	return e
}

// HTTPHandler returns an HTTP handler for the /metrics endpoint.
//
// Example:
//
//	http.Handle("/metrics", exporter.HTTPHandler())
//	http.ListenAndServe(":9090", nil)
func (e *MetricsExporter) HTTPHandler() http.Handler {
	return promhttp.HandlerFor(e.registry, promhttp.HandlerOpts{})
}

// Registry returns the underlying Prometheus registry.
// Useful for custom metric registration or testing.
func (e *MetricsExporter) Registry() *prometheus.Registry {
	return e.registry
}

// UpdateProducerStats updates producer metrics from a statistics JSON string.
// This is typically called when a StatsEvent is received.
//
// The statsJSON parameter is the JSON string from kafka.StatsEvent.Stats.
func (e *MetricsExporter) UpdateProducerStats(clientID string, statsJSON string) error {
	var stats map[string]interface{}
	if err := json.Unmarshal([]byte(statsJSON), &stats); err != nil {
		return err
	}

	// Extract producer-specific metrics
	if txmsgs, ok := stats["txmsgs"].(float64); ok {
		// Total messages transmitted
		// Note: This is a cumulative counter from librdkafka
		e.producerMessagesProduced.WithLabelValues(clientID, "all").Add(txmsgs)
	}

	if txbytes, ok := stats["txbytes"].(float64); ok {
		e.producerBytesProduced.WithLabelValues(clientID).Add(txbytes)
	}

	// Queue size
	if msgcnt, ok := stats["msg_cnt"].(float64); ok {
		e.producerQueueSize.WithLabelValues(clientID).Set(msgcnt)
	}

	// Extract latency (rtt = round-trip time)
	if rtt, ok := stats["rtt"].(map[string]interface{}); ok {
		if p99, ok := rtt["p99"].(float64); ok {
			// Convert microseconds to milliseconds
			e.producerLatencyP99.WithLabelValues(clientID).Set(p99 / 1000.0)
		}
	}

	return nil
}

// UpdateConsumerStats updates consumer metrics from a statistics JSON string.
//
// The statsJSON parameter is the JSON string from kafka.StatsEvent.Stats.
func (e *MetricsExporter) UpdateConsumerStats(clientID string, statsJSON string) error {
	var stats map[string]interface{}
	if err := json.Unmarshal([]byte(statsJSON), &stats); err != nil {
		return err
	}

	// Extract consumer-specific metrics
	if rxmsgs, ok := stats["rxmsgs"].(float64); ok {
		e.consumerMessagesConsumed.WithLabelValues(clientID, "all", "all").Add(rxmsgs)
	}

	if rxbytes, ok := stats["rxbytes"].(float64); ok {
		e.consumerBytesConsumed.WithLabelValues(clientID).Add(rxbytes)
	}

	// Extract per-topic, per-partition lag
	if topics, ok := stats["topics"].(map[string]interface{}); ok {
		for topicName, topicData := range topics {
			if topic, ok := topicData.(map[string]interface{}); ok {
				if partitions, ok := topic["partitions"].(map[string]interface{}); ok {
					for partID, partData := range partitions {
						if part, ok := partData.(map[string]interface{}); ok {
							// Consumer lag = hi_offset - committed_offset
							if hiOffset, ok := part["hi_offset"].(float64); ok {
								if committedOffset, ok := part["committed_offset"].(float64); ok {
									lag := hiOffset - committedOffset
									if lag < 0 {
										lag = 0 // Avoid negative lag
									}
									e.consumerLag.WithLabelValues(clientID, topicName, partID).Set(lag)
								}
							}
						}
					}
				}
			}
		}
	}

	// Connection count
	if brokers, ok := stats["brokers"].(map[string]interface{}); ok {
		connCount := 0
		for _, brokerData := range brokers {
			if broker, ok := brokerData.(map[string]interface{}); ok {
				if state, ok := broker["state"].(string); ok && state == "UP" {
					connCount++
				}
			}
		}
		e.connectionCount.WithLabelValues(clientID).Set(float64(connCount))
	}

	return nil
}

// IncrementProducerErrors increments the producer error counter.
func (e *MetricsExporter) IncrementProducerErrors(clientID string, errorType string) {
	e.producerErrors.WithLabelValues(clientID, errorType).Inc()
}

// IncrementConsumerErrors increments the consumer error counter.
func (e *MetricsExporter) IncrementConsumerErrors(clientID string, errorType string) {
	e.consumerErrors.WithLabelValues(clientID, errorType).Inc()
}

// IncrementConsumerRebalances increments the consumer rebalance counter.
func (e *MetricsExporter) IncrementConsumerRebalances(clientID string, groupID string) {
	e.consumerRebalances.WithLabelValues(clientID, groupID).Inc()
}

// Close cleans up resources used by the exporter.
func (e *MetricsExporter) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()

	for _, client := range e.clients {
		close(client.stopChan)
	}
	e.clients = make(map[string]*clientMetrics)
}
