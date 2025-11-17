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
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// TestMessageCarrierSetAndGet tests the MessageCarrier Set and Get methods
func TestMessageCarrierSetAndGet(t *testing.T) {
	topic := "test-topic"
	msg := &Message{
		TopicPartition: TopicPartition{Topic: &topic, Partition: 0},
		Value:          []byte("test value"),
	}

	carrier := NewMessageCarrier(msg)

	// Test Set
	carrier.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	carrier.Set("tracestate", "congo=t61rcWkgMzE")

	// Test Get
	traceparent := carrier.Get("traceparent")
	if traceparent != "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" {
		t.Errorf("Expected traceparent, got %s", traceparent)
	}

	tracestate := carrier.Get("tracestate")
	if tracestate != "congo=t61rcWkgMzE" {
		t.Errorf("Expected tracestate, got %s", tracestate)
	}

	// Test Get non-existent key
	nonExistent := carrier.Get("non-existent")
	if nonExistent != "" {
		t.Errorf("Expected empty string for non-existent key, got %s", nonExistent)
	}
}

// TestMessageCarrierKeys tests the MessageCarrier Keys method
func TestMessageCarrierKeys(t *testing.T) {
	topic := "test-topic"
	msg := &Message{
		TopicPartition: TopicPartition{Topic: &topic, Partition: 0},
		Value:          []byte("test value"),
	}

	carrier := NewMessageCarrier(msg)

	// Initially no keys
	keys := carrier.Keys()
	if len(keys) != 0 {
		t.Errorf("Expected 0 keys, got %d", len(keys))
	}

	// Add some headers
	carrier.Set("key1", "value1")
	carrier.Set("key2", "value2")

	keys = carrier.Keys()
	if len(keys) != 2 {
		t.Errorf("Expected 2 keys, got %d", len(keys))
	}

	// Check keys are present
	keyMap := make(map[string]bool)
	for _, k := range keys {
		keyMap[k] = true
	}

	if !keyMap["key1"] || !keyMap["key2"] {
		t.Errorf("Expected keys key1 and key2, got %v", keys)
	}
}

// TestMessageCarrierUpdate tests updating an existing header
func TestMessageCarrierUpdate(t *testing.T) {
	topic := "test-topic"
	msg := &Message{
		TopicPartition: TopicPartition{Topic: &topic, Partition: 0},
		Value:          []byte("test value"),
	}

	carrier := NewMessageCarrier(msg)

	// Set initial value
	carrier.Set("key1", "value1")
	if carrier.Get("key1") != "value1" {
		t.Errorf("Expected value1, got %s", carrier.Get("key1"))
	}

	// Update value
	carrier.Set("key1", "value2")
	if carrier.Get("key1") != "value2" {
		t.Errorf("Expected value2, got %s", carrier.Get("key1"))
	}

	// Ensure only one header with this key exists
	count := 0
	for _, h := range msg.Headers {
		if h.Key == "key1" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("Expected 1 header with key1, got %d", count)
	}
}

// TestInjectAndExtractTraceContext tests trace context injection and extraction
func TestInjectAndExtractTraceContext(t *testing.T) {
	// Set up a W3C TraceContext propagator
	otel.SetTextMapPropagator(propagation.TraceContext{})

	// Create a trace context
	ctx := context.Background()

	// Create a mock tracer and span
	tp := trace.NewNoopTracerProvider()
	tracer := tp.Tracer("test")
	ctx, span := tracer.Start(ctx, "test-span")
	defer span.End()

	topic := "test-topic"
	msg := &Message{
		TopicPartition: TopicPartition{Topic: &topic, Partition: 0},
		Value:          []byte("test value"),
	}

	// Inject trace context
	injectTraceContext(ctx, msg)

	// Verify headers were added
	if len(msg.Headers) == 0 {
		t.Fatal("Expected headers to be added")
	}

	// Extract trace context
	extractedCtx := extractTraceContext(context.Background(), msg)

	// Verify context is not nil
	if extractedCtx == nil {
		t.Fatal("Expected non-nil context")
	}
}

// TestMessageCarrierWithExistingHeaders tests MessageCarrier with pre-existing headers
func TestMessageCarrierWithExistingHeaders(t *testing.T) {
	topic := "test-topic"
	msg := &Message{
		TopicPartition: TopicPartition{Topic: &topic, Partition: 0},
		Value:          []byte("test value"),
		Headers: []Header{
			{Key: "existing-key", Value: []byte("existing-value")},
		},
	}

	carrier := NewMessageCarrier(msg)

	// Get existing header
	existing := carrier.Get("existing-key")
	if existing != "existing-value" {
		t.Errorf("Expected existing-value, got %s", existing)
	}

	// Add new header
	carrier.Set("new-key", "new-value")

	// Verify both headers exist
	if len(msg.Headers) != 2 {
		t.Errorf("Expected 2 headers, got %d", len(msg.Headers))
	}

	// Verify new header
	newVal := carrier.Get("new-key")
	if newVal != "new-value" {
		t.Errorf("Expected new-value, got %s", newVal)
	}

	// Verify existing header still exists
	existing = carrier.Get("existing-key")
	if existing != "existing-value" {
		t.Errorf("Expected existing-value, got %s", existing)
	}
}

// TestStartProducerSpan tests the startProducerSpan function
func TestStartProducerSpan(t *testing.T) {
	// Set up a noop tracer
	otel.SetTracerProvider(trace.NewNoopTracerProvider())

	ctx := context.Background()
	topic := "test-topic"
	msg := &Message{
		TopicPartition: TopicPartition{Topic: &topic, Partition: 0},
		Value:          []byte("test value"),
	}

	newCtx, span := startProducerSpan(ctx, msg)
	defer span.End()

	if newCtx == nil {
		t.Error("Expected non-nil context")
	}

	if span == nil {
		t.Error("Expected non-nil span")
	}
}

// TestStartConsumerSpan tests the startConsumerSpan function
func TestStartConsumerSpan(t *testing.T) {
	// Set up a noop tracer
	otel.SetTracerProvider(trace.NewNoopTracerProvider())

	ctx := context.Background()
	topic := "test-topic"
	msg := &Message{
		TopicPartition: TopicPartition{Topic: &topic, Partition: 0, Offset: 123},
		Value:          []byte("test value"),
	}

	newCtx, span := startConsumerSpan(ctx, msg, "test-group")
	defer span.End()

	if newCtx == nil {
		t.Error("Expected non-nil context")
	}

	if span == nil {
		t.Error("Expected non-nil span")
	}
}
