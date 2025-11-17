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

package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestNewMetricsExporter(t *testing.T) {
	exporter := NewMetricsExporter("test")

	if exporter == nil {
		t.Fatal("NewMetricsExporter returned nil")
	}

	if exporter.namespace != "test" {
		t.Errorf("Expected namespace 'test', got '%s'", exporter.namespace)
	}

	if exporter.registry == nil {
		t.Error("Registry is nil")
	}
}

func TestUpdateProducerStats(t *testing.T) {
	exporter := NewMetricsExporter("test")

	statsJSON := `{
		"name": "producer-1",
		"type": "producer",
		"txmsgs": 1000,
		"txbytes": 500000,
		"msg_cnt": 42,
		"rtt": {
			"p50": 5000,
			"p95": 10000,
			"p99": 15000
		}
	}`

	err := exporter.UpdateProducerStats("producer-1", statsJSON)
	if err != nil {
		t.Fatalf("UpdateProducerStats failed: %v", err)
	}

	// Verify queue size was set
	queueSize := testutil.ToFloat64(exporter.producerQueueSize.WithLabelValues("producer-1"))
	if queueSize != 42 {
		t.Errorf("Expected queue size 42, got %f", queueSize)
	}

	// Verify latency was set (converted from microseconds to milliseconds)
	latency := testutil.ToFloat64(exporter.producerLatencyP99.WithLabelValues("producer-1"))
	if latency != 15.0 {
		t.Errorf("Expected latency 15.0ms, got %f", latency)
	}
}

func TestUpdateConsumerStats(t *testing.T) {
	exporter := NewMetricsExporter("test")

	statsJSON := `{
		"name": "consumer-1",
		"type": "consumer",
		"rxmsgs": 5000,
		"rxbytes": 2500000,
		"topics": {
			"test-topic": {
				"partitions": {
					"0": {
						"hi_offset": 10000,
						"committed_offset": 9500
					},
					"1": {
						"hi_offset": 12000,
						"committed_offset": 11000
					}
				}
			}
		},
		"brokers": {
			"broker1": {
				"state": "UP"
			},
			"broker2": {
				"state": "UP"
			},
			"broker3": {
				"state": "DOWN"
			}
		}
	}`

	err := exporter.UpdateConsumerStats("consumer-1", statsJSON)
	if err != nil {
		t.Fatalf("UpdateConsumerStats failed: %v", err)
	}

	// Verify lag for partition 0
	lag0 := testutil.ToFloat64(exporter.consumerLag.WithLabelValues("consumer-1", "test-topic", "0"))
	if lag0 != 500 {
		t.Errorf("Expected lag 500 for partition 0, got %f", lag0)
	}

	// Verify lag for partition 1
	lag1 := testutil.ToFloat64(exporter.consumerLag.WithLabelValues("consumer-1", "test-topic", "1"))
	if lag1 != 1000 {
		t.Errorf("Expected lag 1000 for partition 1, got %f", lag1)
	}

	// Verify connection count (2 UP brokers)
	connCount := testutil.ToFloat64(exporter.connectionCount.WithLabelValues("consumer-1"))
	if connCount != 2 {
		t.Errorf("Expected 2 connections, got %f", connCount)
	}
}

func TestIncrementProducerErrors(t *testing.T) {
	exporter := NewMetricsExporter("test")

	exporter.IncrementProducerErrors("producer-1", "timeout")
	exporter.IncrementProducerErrors("producer-1", "timeout")
	exporter.IncrementProducerErrors("producer-1", "network")

	timeoutErrors := testutil.ToFloat64(exporter.producerErrors.WithLabelValues("producer-1", "timeout"))
	if timeoutErrors != 2 {
		t.Errorf("Expected 2 timeout errors, got %f", timeoutErrors)
	}

	networkErrors := testutil.ToFloat64(exporter.producerErrors.WithLabelValues("producer-1", "network"))
	if networkErrors != 1 {
		t.Errorf("Expected 1 network error, got %f", networkErrors)
	}
}

func TestIncrementConsumerErrors(t *testing.T) {
	exporter := NewMetricsExporter("test")

	exporter.IncrementConsumerErrors("consumer-1", "offset_out_of_range")
	exporter.IncrementConsumerErrors("consumer-1", "offset_out_of_range")

	errors := testutil.ToFloat64(exporter.consumerErrors.WithLabelValues("consumer-1", "offset_out_of_range"))
	if errors != 2 {
		t.Errorf("Expected 2 errors, got %f", errors)
	}
}

func TestIncrementConsumerRebalances(t *testing.T) {
	exporter := NewMetricsExporter("test")

	exporter.IncrementConsumerRebalances("consumer-1", "my-group")
	exporter.IncrementConsumerRebalances("consumer-1", "my-group")
	exporter.IncrementConsumerRebalances("consumer-1", "my-group")

	rebalances := testutil.ToFloat64(exporter.consumerRebalances.WithLabelValues("consumer-1", "my-group"))
	if rebalances != 3 {
		t.Errorf("Expected 3 rebalances, got %f", rebalances)
	}
}

func TestHTTPHandler(t *testing.T) {
	exporter := NewMetricsExporter("test")

	handler := exporter.HTTPHandler()
	if handler == nil {
		t.Error("HTTPHandler returned nil")
	}
}

func TestMetricNames(t *testing.T) {
	exporter := NewMetricsExporter("myapp")

	// Update some metrics
	exporter.IncrementProducerErrors("prod-1", "timeout")
	exporter.IncrementConsumerRebalances("cons-1", "group-1")

	// Collect metrics and verify naming
	metricFamilies, err := exporter.registry.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	foundProducerErrors := false
	foundConsumerRebalances := false

	for _, mf := range metricFamilies {
		name := *mf.Name
		if name == "myapp_producer_errors_total" {
			foundProducerErrors = true
		}
		if name == "myapp_consumer_rebalances_total" {
			foundConsumerRebalances = true
		}
	}

	if !foundProducerErrors {
		t.Error("Producer errors metric not found with correct name")
	}
	if !foundConsumerRebalances {
		t.Error("Consumer rebalances metric not found with correct name")
	}
}

func TestInvalidJSON(t *testing.T) {
	exporter := NewMetricsExporter("test")

	err := exporter.UpdateProducerStats("prod-1", "invalid json {")
	if err == nil {
		t.Error("Expected error for invalid JSON, got nil")
	}

	err = exporter.UpdateConsumerStats("cons-1", "not json at all")
	if err == nil {
		t.Error("Expected error for invalid JSON, got nil")
	}
}

func TestNegativeLag(t *testing.T) {
	exporter := NewMetricsExporter("test")

	// Scenario where committed_offset > hi_offset (shouldn't happen but handle gracefully)
	statsJSON := `{
		"topics": {
			"test-topic": {
				"partitions": {
					"0": {
						"hi_offset": 100,
						"committed_offset": 150
					}
				}
			}
		}
	}`

	err := exporter.UpdateConsumerStats("consumer-1", statsJSON)
	if err != nil {
		t.Fatalf("UpdateConsumerStats failed: %v", err)
	}

	// Lag should be 0 (not negative)
	lag := testutil.ToFloat64(exporter.consumerLag.WithLabelValues("consumer-1", "test-topic", "0"))
	if lag != 0 {
		t.Errorf("Expected lag 0 for negative scenario, got %f", lag)
	}
}

func TestClose(t *testing.T) {
	exporter := NewMetricsExporter("test")

	// Add some mock clients
	exporter.clients["client1"] = &clientMetrics{
		clientID:   "client1",
		clientType: "producer",
		stopChan:   make(chan struct{}),
	}

	exporter.Close()

	if len(exporter.clients) != 0 {
		t.Errorf("Expected 0 clients after Close(), got %d", len(exporter.clients))
	}
}

// Benchmark metric updates
func BenchmarkUpdateProducerStats(b *testing.B) {
	exporter := NewMetricsExporter("bench")
	statsJSON := `{"txmsgs": 1000, "txbytes": 500000, "msg_cnt": 42, "rtt": {"p99": 15000}}`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		exporter.UpdateProducerStats("prod-1", statsJSON)
	}
}

func BenchmarkUpdateConsumerStats(b *testing.B) {
	exporter := NewMetricsExporter("bench")
	statsJSON := `{
		"rxmsgs": 5000,
		"topics": {
			"topic1": {
				"partitions": {
					"0": {"hi_offset": 10000, "committed_offset": 9500}
				}
			}
		}
	}`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		exporter.UpdateConsumerStats("cons-1", statsJSON)
	}
}

func BenchmarkIncrementErrors(b *testing.B) {
	exporter := NewMetricsExporter("bench")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		exporter.IncrementProducerErrors("prod-1", "timeout")
	}
}

// Test that metrics can be scraped in Prometheus format
func TestPrometheusFormat(t *testing.T) {
	exporter := NewMetricsExporter("testapp")

	// Set some metrics
	exporter.IncrementProducerErrors("prod-1", "timeout")
	exporter.UpdateProducerStats("prod-1", `{"msg_cnt": 100}`)

	// Gather metrics
	metricFamilies, err := exporter.registry.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	// Verify we have metrics
	if len(metricFamilies) == 0 {
		t.Error("No metrics gathered")
	}

	// Check that metric names start with namespace
	for _, mf := range metricFamilies {
		name := *mf.Name
		if !strings.HasPrefix(name, "testapp_") {
			t.Errorf("Metric name '%s' doesn't have namespace prefix 'testapp_'", name)
		}
	}
}
