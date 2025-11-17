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

// OpenTelemetry tracing example with Kafka producer and consumer
// This example demonstrates how to propagate trace context through Kafka messages
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
)

// initTracer initializes an OpenTelemetry tracer with Jaeger exporter
func initTracer(serviceName string) (*sdktrace.TracerProvider, error) {
	// Create Jaeger exporter
	// Note: This requires Jaeger to be running locally
	// Run: docker run -d --name jaeger -p 16686:16686 -p 14268:14268 jaegertracing/all-in-one:latest
	exporter, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint("http://localhost:14268/api/traces")))
	if err != nil {
		return nil, fmt.Errorf("failed to create Jaeger exporter: %w", err)
	}

	// Create resource with service name
	res, err := resource.New(
		context.Background(),
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// Create tracer provider
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	// Set global tracer provider and propagator
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tp, nil
}

// produceMessages demonstrates producing messages with trace context
func produceMessages(broker, topic string) error {
	fmt.Println("=== Producer Example ===")

	// Create producer with OpenTelemetry enabled
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
		"go.otel.enabled":   true, // Enable OpenTelemetry tracing
	})
	if err != nil {
		return fmt.Errorf("failed to create producer: %w", err)
	}
	defer p.Close()

	// Create a root span to simulate an incoming HTTP request or other operation
	tracer := otel.Tracer("kafka-producer-example")
	ctx := context.Background()
	ctx, rootSpan := tracer.Start(ctx, "http-request",
		trace.WithAttributes(
			attribute.String("http.method", "POST"),
			attribute.String("http.url", "/api/orders"),
		),
	)
	defer rootSpan.End()

	// Handle delivery reports in a goroutine
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("❌ Delivery failed: %v\n", ev.TopicPartition.Error)
				} else {
					fmt.Printf("✅ Message delivered to %v [partition %d] at offset %v\n",
						*ev.TopicPartition.Topic,
						ev.TopicPartition.Partition,
						ev.TopicPartition.Offset)
				}
			}
		}
	}()

	// Produce messages with trace context
	for i := 0; i < 5; i++ {
		value := fmt.Sprintf("Order #%d", i+1)
		key := fmt.Sprintf("order-%d", i+1)

		// Create a child span for each message production
		_, msgSpan := tracer.Start(ctx, "process-order",
			trace.WithAttributes(
				attribute.String("order.id", key),
				attribute.String("order.value", value),
			),
		)

		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte(key),
			Value:          []byte(value),
		}

		// ProduceWithContext will inject trace context into message headers
		if err := p.ProduceWithContext(ctx, msg, nil); err != nil {
			fmt.Printf("❌ Failed to produce message: %v\n", err)
			msgSpan.End()
			continue
		}

		fmt.Printf("📤 Produced message: key=%s, value=%s\n", key, value)
		msgSpan.End()

		time.Sleep(1 * time.Second)
	}

	// Wait for all messages to be delivered
	fmt.Println("⏳ Flushing producer...")
	p.Flush(10 * 1000)
	fmt.Println("✅ All messages flushed")

	return nil
}

// consumeMessages demonstrates consuming messages with trace context
func consumeMessages(broker, topic, groupID string) error {
	fmt.Println("\n=== Consumer Example ===")

	// Create consumer with OpenTelemetry enabled
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  broker,
		"group.id":           groupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
		"go.otel.enabled":    true, // Enable OpenTelemetry tracing
	})
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}
	defer c.Close()

	// Subscribe to topic
	if err := c.SubscribeTopics([]string{topic}, nil); err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}

	fmt.Printf("📥 Consuming from topic '%s'\n", topic)

	// Set up signal handler for graceful shutdown
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	tracer := otel.Tracer("kafka-consumer-example")
	run := true

	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("\n🛑 Caught signal %v: terminating\n", sig)
			run = false

		default:
			// ReadMessageWithContext extracts trace context from message headers
			// and creates a consumer span linked to the producer span
			msg, msgCtx, span, err := c.ReadMessageWithContext(context.Background(), 100*time.Millisecond)
			if err != nil {
				if err.(kafka.Error).Code() == kafka.ErrTimedOut {
					continue
				}
				fmt.Printf("❌ Consumer error: %v\n", err)
				continue
			}

			// Process the message within the trace context
			func(ctx context.Context, span trace.Span) {
				defer span.End()

				// Create a child span for message processing
				_, processingSpan := tracer.Start(ctx, "process-message",
					trace.WithAttributes(
						attribute.String("message.key", string(msg.Key)),
						attribute.Int("message.length", len(msg.Value)),
					),
				)
				defer processingSpan.End()

				fmt.Printf("📨 Received message:\n")
				fmt.Printf("   Topic:     %s [partition %d] at offset %v\n",
					*msg.TopicPartition.Topic,
					msg.TopicPartition.Partition,
					msg.TopicPartition.Offset)
				fmt.Printf("   Key:       %s\n", string(msg.Key))
				fmt.Printf("   Value:     %s\n", string(msg.Value))

				// Check for trace headers
				for _, header := range msg.Headers {
					if header.Key == "traceparent" {
						fmt.Printf("   🔗 Trace:   %s\n", string(header.Value))
						break
					}
				}

				// Simulate message processing
				time.Sleep(100 * time.Millisecond)

				// Commit offset
				if _, err := c.CommitMessage(msg); err != nil {
					fmt.Printf("❌ Failed to commit offset: %v\n", err)
					processingSpan.RecordError(err)
				}
			}(msgCtx, span)

			fmt.Println()
		}
	}

	return nil
}

func main() {
	// Check command line arguments
	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "Usage: %s <broker> <topic> <mode>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  mode: 'producer' or 'consumer'\n")
		fmt.Fprintf(os.Stderr, "Example: %s localhost:9092 otel-test producer\n", os.Args[0])
		os.Exit(1)
	}

	broker := os.Args[1]
	topic := os.Args[2]
	mode := os.Args[3]

	// Initialize tracer
	serviceName := fmt.Sprintf("kafka-%s-example", mode)
	tp, err := initTracer(serviceName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize tracer: %v\n", err)
		fmt.Fprintf(os.Stderr, "Note: Make sure Jaeger is running (docker run -d --name jaeger -p 16686:16686 -p 14268:14268 jaegertracing/all-in-one:latest)\n")
		os.Exit(1)
	}
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "Error shutting down tracer provider: %v\n", err)
		}
	}()

	fmt.Println("🔭 OpenTelemetry Kafka Example")
	fmt.Printf("📊 View traces at: http://localhost:16686\n")
	fmt.Printf("🎯 Service: %s\n", serviceName)
	fmt.Println()

	// Run producer or consumer
	switch mode {
	case "producer":
		if err := produceMessages(broker, topic); err != nil {
			fmt.Fprintf(os.Stderr, "Producer error: %v\n", err)
			os.Exit(1)
		}
	case "consumer":
		groupID := fmt.Sprintf("otel-example-group-%d", time.Now().Unix())
		if err := consumeMessages(broker, topic, groupID); err != nil {
			fmt.Fprintf(os.Stderr, "Consumer error: %v\n", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "Invalid mode: %s (use 'producer' or 'consumer')\n", mode)
		os.Exit(1)
	}
}
