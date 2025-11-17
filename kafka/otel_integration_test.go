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

package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// TestOpenTelemetryIntegration tests end-to-end OpenTelemetry trace propagation
func TestOpenTelemetryIntegration(t *testing.T) {
	// Skip if not running integration tests
	if !testconf.DockerNeeded && !testconf.DockerExists {
		t.Skipf("Skipping OpenTelemetry integration test (no broker available)")
	}

	// Set up in-memory span exporter for testing
	spanRecorder := tracetest.NewSpanRecorder()
	tp := trace.NewTracerProvider(
		trace.WithSpanProcessor(spanRecorder),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	// Create test topic
	topic := fmt.Sprintf("otel-test-%d", time.Now().Unix())

	// Create producer with OTel enabled
	producerConfig := &ConfigMap{
		"bootstrap.servers": testconf.Brokers,
		"go.otel.enabled":   true,
	}

	producer, err := NewProducer(producerConfig)
	if err != nil {
		t.Fatalf("Failed to create producer: %s", err)
	}
	defer producer.Close()

	// Create consumer with OTel enabled
	consumerConfig := &ConfigMap{
		"bootstrap.servers":  testconf.Brokers,
		"group.id":           fmt.Sprintf("otel-test-group-%d", time.Now().Unix()),
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
		"go.otel.enabled":    true,
	}

	consumer, err := NewConsumer(consumerConfig)
	if err != nil {
		t.Fatalf("Failed to create consumer: %s", err)
	}
	defer consumer.Close()

	// Subscribe to topic
	err = consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		t.Fatalf("Failed to subscribe: %s", err)
	}

	// Create a root span to simulate an incoming request
	ctx := context.Background()
	tracer := otel.Tracer("test")
	ctx, rootSpan := tracer.Start(ctx, "test-request")

	// Produce a message with trace context
	testValue := []byte("test-message-with-trace")
	msg := &Message{
		TopicPartition: TopicPartition{Topic: &topic, Partition: PartitionAny},
		Value:          testValue,
		Key:            []byte("test-key"),
	}

	err = producer.ProduceWithContext(ctx, msg, nil)
	if err != nil {
		t.Fatalf("Failed to produce message: %s", err)
	}

	// Flush to ensure message is sent
	producer.Flush(10000)

	// End root span
	rootSpan.End()

	// Consume the message with trace context
	consumerCtx := context.Background()
	consumedMsg, msgCtx, consumerSpan, err := consumer.ReadMessageWithContext(consumerCtx, 30*time.Second)
	if err != nil {
		t.Fatalf("Failed to consume message: %s", err)
	}

	// Verify message content
	if string(consumedMsg.Value) != string(testValue) {
		t.Errorf("Expected value %s, got %s", testValue, consumedMsg.Value)
	}

	// End consumer span
	if consumerSpan != nil {
		consumerSpan.End()
	}

	// Verify trace context was propagated
	if msgCtx == nil {
		t.Fatal("Expected non-nil message context")
	}

	// Verify headers contain trace context
	traceparentFound := false
	for _, header := range consumedMsg.Headers {
		if header.Key == "traceparent" {
			traceparentFound = true
			t.Logf("Found traceparent header: %s", string(header.Value))
			break
		}
	}

	if !traceparentFound {
		t.Error("Expected traceparent header to be present")
	}

	// Force flush spans
	if err := tp.ForceFlush(context.Background()); err != nil {
		t.Logf("Failed to flush spans: %s", err)
	}

	// Verify spans were recorded
	spans := spanRecorder.Ended()
	if len(spans) < 2 {
		t.Errorf("Expected at least 2 spans (producer and consumer), got %d", len(spans))
	}

	// Log spans for debugging
	for i, span := range spans {
		t.Logf("Span %d: Name=%s, Kind=%s", i, span.Name(), span.SpanKind())
	}
}

// TestProducerWithoutContext tests that producer works without context (backward compatibility)
func TestProducerWithoutContext(t *testing.T) {
	if !testconf.DockerNeeded && !testconf.DockerExists {
		t.Skipf("Skipping test (no broker available)")
	}

	// Create producer with OTel enabled
	producerConfig := &ConfigMap{
		"bootstrap.servers": testconf.Brokers,
		"go.otel.enabled":   true,
	}

	producer, err := NewProducer(producerConfig)
	if err != nil {
		t.Fatalf("Failed to create producer: %s", err)
	}
	defer producer.Close()

	topic := fmt.Sprintf("otel-test-no-ctx-%d", time.Now().Unix())

	// Produce message with nil context (should not panic)
	msg := &Message{
		TopicPartition: TopicPartition{Topic: &topic, Partition: PartitionAny},
		Value:          []byte("test-message"),
	}

	err = producer.ProduceWithContext(nil, msg, nil)
	if err != nil {
		t.Fatalf("Failed to produce message: %s", err)
	}

	// Also test old Produce method still works
	err = producer.Produce(msg, nil)
	if err != nil {
		t.Fatalf("Failed to produce message with old API: %s", err)
	}

	producer.Flush(5000)
}

// TestConsumerWithoutContext tests that consumer works without context (backward compatibility)
func TestConsumerWithoutContext(t *testing.T) {
	if !testconf.DockerNeeded && !testconf.DockerExists {
		t.Skipf("Skipping test (no broker available)")
	}

	// Create consumer with OTel enabled
	consumerConfig := &ConfigMap{
		"bootstrap.servers":  testconf.Brokers,
		"group.id":           fmt.Sprintf("otel-test-group-no-ctx-%d", time.Now().Unix()),
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
		"go.otel.enabled":    true,
	}

	consumer, err := NewConsumer(consumerConfig)
	if err != nil {
		t.Fatalf("Failed to create consumer: %s", err)
	}
	defer consumer.Close()

	// Test that ReadMessageWithContext works with nil context
	_, ctx, span, err := consumer.ReadMessageWithContext(nil, 1*time.Second)
	if err != nil {
		// Timeout is expected, just verify it doesn't panic
		if err.(Error).Code() != ErrTimedOut {
			t.Fatalf("Unexpected error: %s", err)
		}
	}

	if span != nil {
		span.End()
	}

	if ctx == nil {
		t.Error("Expected non-nil context even with nil input")
	}

	// Test that old ReadMessage method still works
	_, err = consumer.ReadMessage(1 * time.Second)
	if err != nil {
		if err.(Error).Code() != ErrTimedOut {
			t.Fatalf("Unexpected error with old API: %s", err)
		}
	}
}

// TestOtelDisabled tests that OTel is disabled by default
func TestOtelDisabled(t *testing.T) {
	if !testconf.DockerNeeded && !testconf.DockerExists {
		t.Skipf("Skipping test (no broker available)")
	}

	// Create producer without OTel enabled (default)
	producerConfig := &ConfigMap{
		"bootstrap.servers": testconf.Brokers,
	}

	producer, err := NewProducer(producerConfig)
	if err != nil {
		t.Fatalf("Failed to create producer: %s", err)
	}
	defer producer.Close()

	// Verify OTel is disabled
	if producer.handle.otelEnabled {
		t.Error("Expected otelEnabled to be false by default")
	}

	topic := fmt.Sprintf("otel-test-disabled-%d", time.Now().Unix())

	// Produce message - should not add trace headers
	ctx := context.Background()
	tracer := otel.Tracer("test")
	ctx, span := tracer.Start(ctx, "test-span")
	defer span.End()

	msg := &Message{
		TopicPartition: TopicPartition{Topic: &topic, Partition: PartitionAny},
		Value:          []byte("test-message"),
	}

	err = producer.ProduceWithContext(ctx, msg, nil)
	if err != nil {
		t.Fatalf("Failed to produce message: %s", err)
	}

	// Verify no trace headers were added
	traceparentFound := false
	for _, header := range msg.Headers {
		if header.Key == "traceparent" {
			traceparentFound = true
			break
		}
	}

	if traceparentFound {
		t.Error("Expected no traceparent header when OTel is disabled")
	}
}
