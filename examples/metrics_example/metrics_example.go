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

// Prometheus metrics exporter example
//
// This example demonstrates how to export Kafka client metrics to Prometheus.
// The metrics are exposed via an HTTP /metrics endpoint that can be scraped
// by a Prometheus server.
//
// Usage:
//   1. Start this example: go run metrics_example.go
//   2. View metrics: curl http://localhost:9090/metrics
//   3. Configure Prometheus to scrape http://localhost:9090/metrics
package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka/metrics"
)

func main() {
	fmt.Println("=== Prometheus Metrics Exporter Example ===\n")

	// Create metrics exporter
	fmt.Println("Creating metrics exporter with namespace 'kafka_go_example'")
	exporter := metrics.NewMetricsExporter("kafka_go_example")

	// Start HTTP server for /metrics endpoint
	http.Handle("/metrics", exporter.HTTPHandler())
	go func() {
		fmt.Println("Starting HTTP server on :9090")
		fmt.Println("Metrics available at: http://localhost:9090/metrics")
		fmt.Println()
		if err := http.ListenAndServe(":9090", nil); err != nil {
			log.Fatalf("Failed to start HTTP server: %v", err)
		}
	}()

	// Give HTTP server time to start
	time.Sleep(100 * time.Millisecond)

	// Run producer example
	fmt.Println("Running producer example...")
	runProducerExample(exporter)

	// Run consumer example
	fmt.Println("\nRunning consumer example...")
	runConsumerExample(exporter)

	// Keep server running
	fmt.Println("\n=== Metrics server running ===")
	fmt.Println("View metrics: curl http://localhost:9090/metrics")
	fmt.Println("Press Ctrl+C to exit")
	fmt.Println()

	// Wait for interrupt
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm

	fmt.Println("\nShutting down...")
	exporter.Close()
}

func runProducerExample(exporter *metrics.MetricsExporter) {
	// Create producer
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":    "localhost:9092",
		"client.id":            "metrics-producer-example",
		"statistics.interval.ms": 5000, // Enable statistics every 5 seconds
	})

	if err != nil {
		fmt.Printf("Note: Could not create producer: %v\n", err)
		fmt.Println("(This is normal if Kafka is not running)")
		return
	}
	defer p.Close()

	// Collect statistics events
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					// Track errors
					exporter.IncrementProducerErrors("metrics-producer-example", "delivery_failed")
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition.Error)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}

			case *kafka.Stats:
				// Update producer metrics from statistics
				err := exporter.UpdateProducerStats("metrics-producer-example", ev.String())
				if err != nil {
					fmt.Printf("Failed to update metrics: %v\n", err)
				} else {
					fmt.Println("✓ Producer metrics updated")
				}

			case kafka.Error:
				exporter.IncrementProducerErrors("metrics-producer-example", "kafka_error")
				fmt.Printf("Producer error: %v\n", ev)
			}
		}
	}()

	// Produce some messages
	topic := "metrics-test-topic"
	for i := 0; i < 10; i++ {
		value := fmt.Sprintf("Message %d", i)
		err := p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(value),
		}, nil)

		if err != nil {
			exporter.IncrementProducerErrors("metrics-producer-example", "produce_failed")
			fmt.Printf("Failed to produce message: %v\n", err)
		}
	}

	// Wait for deliveries
	p.Flush(5000)
	fmt.Println("Producer example complete")
}

func runConsumerExample(exporter *metrics.MetricsExporter) {
	// Create consumer
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":    "localhost:9092",
		"group.id":             "metrics-consumer-group",
		"client.id":            "metrics-consumer-example",
		"auto.offset.reset":    "earliest",
		"statistics.interval.ms": 5000, // Enable statistics every 5 seconds
	})

	if err != nil {
		fmt.Printf("Note: Could not create consumer: %v\n", err)
		fmt.Println("(This is normal if Kafka is not running)")
		return
	}
	defer c.Close()

	// Subscribe to topic
	err = c.SubscribeTopics([]string{"metrics-test-topic"}, nil)
	if err != nil {
		fmt.Printf("Failed to subscribe: %v\n", err)
		return
	}

	// Collect events
	done := make(chan bool)
	go func() {
		msgCount := 0
		timeout := time.After(10 * time.Second)

		for {
			select {
			case <-timeout:
				done <- true
				return

			default:
				ev := c.Poll(100)
				if ev == nil {
					continue
				}

				switch e := ev.(type) {
				case *kafka.Message:
					msgCount++
					fmt.Printf("Consumed message: %s\n", string(e.Value))

				case kafka.Error:
					exporter.IncrementConsumerErrors("metrics-consumer-example", "kafka_error")
					fmt.Printf("Consumer error: %v\n", e)

				case *kafka.Stats:
					// Update consumer metrics from statistics
					err := exporter.UpdateConsumerStats("metrics-consumer-example", e.String())
					if err != nil {
						fmt.Printf("Failed to update metrics: %v\n", err)
					} else {
						fmt.Println("✓ Consumer metrics updated")
					}

				case kafka.AssignedPartitions:
					c.Assign(e.Partitions)
					fmt.Printf("Partitions assigned: %v\n", e.Partitions)

				case kafka.RevokedPartitions:
					c.Unassign()
					exporter.IncrementConsumerRebalances("metrics-consumer-example", "metrics-consumer-group")
					fmt.Println("✓ Rebalance tracked")
				}
			}
		}
	}()

	<-done
	fmt.Println("Consumer example complete")
}

// Example Grafana Dashboard JSON
const grafanaDashboard = `
{
  "dashboard": {
    "title": "Kafka Go Client Metrics",
    "panels": [
      {
        "title": "Producer Message Rate",
        "targets": [
          {
            "expr": "rate(kafka_go_example_producer_messages_total[5m])"
          }
        ]
      },
      {
        "title": "Producer Latency (P99)",
        "targets": [
          {
            "expr": "kafka_go_example_producer_latency_p99_milliseconds"
          }
        ]
      },
      {
        "title": "Consumer Lag",
        "targets": [
          {
            "expr": "kafka_go_example_consumer_lag_messages"
          }
        ]
      },
      {
        "title": "Error Rate",
        "targets": [
          {
            "expr": "rate(kafka_go_example_producer_errors_total[5m])"
          },
          {
            "expr": "rate(kafka_go_example_consumer_errors_total[5m])"
          }
        ]
      }
    ]
  }
}
`

// Example Prometheus scrape configuration:
//
// scrape_configs:
//   - job_name: 'kafka-go-client'
//     static_configs:
//       - targets: ['localhost:9090']
//
// Example metrics output:
//
// # HELP kafka_go_example_producer_messages_total Total number of messages produced
// # TYPE kafka_go_example_producer_messages_total counter
// kafka_go_example_producer_messages_total{client_id="metrics-producer-example",topic="all"} 1000
//
// # HELP kafka_go_example_producer_latency_p99_milliseconds 99th percentile produce latency in milliseconds
// # TYPE kafka_go_example_producer_latency_p99_milliseconds gauge
// kafka_go_example_producer_latency_p99_milliseconds{client_id="metrics-producer-example"} 15.5
//
// # HELP kafka_go_example_consumer_lag_messages Consumer lag (messages behind high water mark)
// # TYPE kafka_go_example_consumer_lag_messages gauge
// kafka_go_example_consumer_lag_messages{client_id="metrics-consumer-example",partition="0",topic="test-topic"} 500
//
// # HELP kafka_go_example_producer_errors_total Total number of producer errors
// # TYPE kafka_go_example_producer_errors_total counter
// kafka_go_example_producer_errors_total{client_id="metrics-producer-example",error_type="timeout"} 5
