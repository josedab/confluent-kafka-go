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

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	// OpenTelemetry instrumentation name
	instrumentationName = "github.com/confluentinc/confluent-kafka-go/v2/kafka"

	// Semantic convention attributes for Kafka messaging
	// Following: https://opentelemetry.io/docs/specs/semconv/messaging/kafka/
	attrMessagingSystem            = "messaging.system"
	attrMessagingDestination       = "messaging.destination.name"
	attrMessagingDestinationKind   = "messaging.destination.kind"
	attrMessagingOperation         = "messaging.operation"
	attrMessagingKafkaPartition    = "messaging.kafka.destination.partition"
	attrMessagingKafkaOffset       = "messaging.kafka.message.offset"
	attrMessagingKafkaConsumerGroup = "messaging.kafka.consumer.group"
	attrMessagingMessageID         = "messaging.message.id"

	// Operation names
	operationPublish = "publish"
	operationReceive = "receive"
)

// MessageCarrier adapts a Kafka Message to the OpenTelemetry TextMapCarrier interface
// for propagating trace context through message headers.
type MessageCarrier struct {
	msg *Message
}

// NewMessageCarrier creates a new MessageCarrier for the given message.
func NewMessageCarrier(msg *Message) *MessageCarrier {
	return &MessageCarrier{msg: msg}
}

// Get retrieves the value for a given key from message headers.
func (c *MessageCarrier) Get(key string) string {
	if c.msg.Headers == nil {
		return ""
	}

	for _, header := range c.msg.Headers {
		if header.Key == key {
			return string(header.Value)
		}
	}

	return ""
}

// Set adds or updates a header with the given key and value.
func (c *MessageCarrier) Set(key, value string) {
	if c.msg.Headers == nil {
		c.msg.Headers = []Header{}
	}

	// Check if header already exists and update it
	for i := range c.msg.Headers {
		if c.msg.Headers[i].Key == key {
			c.msg.Headers[i].Value = []byte(value)
			return
		}
	}

	// Add new header
	c.msg.Headers = append(c.msg.Headers, Header{
		Key:   key,
		Value: []byte(value),
	})
}

// Keys returns all header keys.
func (c *MessageCarrier) Keys() []string {
	if c.msg.Headers == nil {
		return []string{}
	}

	keys := make([]string, 0, len(c.msg.Headers))
	for _, header := range c.msg.Headers {
		keys = append(keys, header.Key)
	}

	return keys
}

// injectTraceContext injects the trace context from ctx into the message headers.
func injectTraceContext(ctx context.Context, msg *Message) {
	propagator := otel.GetTextMapPropagator()
	carrier := NewMessageCarrier(msg)
	propagator.Inject(ctx, carrier)
}

// extractTraceContext extracts the trace context from message headers and returns
// a new context with the trace context.
func extractTraceContext(ctx context.Context, msg *Message) context.Context {
	propagator := otel.GetTextMapPropagator()
	carrier := NewMessageCarrier(msg)
	return propagator.Extract(ctx, carrier)
}

// startProducerSpan creates a span for a produce operation.
func startProducerSpan(ctx context.Context, msg *Message) (context.Context, trace.Span) {
	tracer := otel.Tracer(instrumentationName)

	attrs := []attribute.KeyValue{
		attribute.String(attrMessagingSystem, "kafka"),
		attribute.String(attrMessagingDestinationKind, "topic"),
		attribute.String(attrMessagingOperation, operationPublish),
	}

	if msg.TopicPartition.Topic != nil {
		attrs = append(attrs, attribute.String(attrMessagingDestination, *msg.TopicPartition.Topic))
	}

	if msg.TopicPartition.Partition != PartitionAny {
		attrs = append(attrs, attribute.Int(attrMessagingKafkaPartition, int(msg.TopicPartition.Partition)))
	}

	spanName := "kafka.produce"
	if msg.TopicPartition.Topic != nil {
		spanName = *msg.TopicPartition.Topic + " publish"
	}

	ctx, span := tracer.Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(attrs...),
	)

	return ctx, span
}

// startConsumerSpan creates a span for a consume operation.
func startConsumerSpan(ctx context.Context, msg *Message, groupID string) (context.Context, trace.Span) {
	tracer := otel.Tracer(instrumentationName)

	attrs := []attribute.KeyValue{
		attribute.String(attrMessagingSystem, "kafka"),
		attribute.String(attrMessagingDestinationKind, "topic"),
		attribute.String(attrMessagingOperation, operationReceive),
	}

	if msg.TopicPartition.Topic != nil {
		attrs = append(attrs, attribute.String(attrMessagingDestination, *msg.TopicPartition.Topic))
	}

	if msg.TopicPartition.Partition != PartitionAny {
		attrs = append(attrs, attribute.Int(attrMessagingKafkaPartition, int(msg.TopicPartition.Partition)))
	}

	if msg.TopicPartition.Offset != OffsetInvalid {
		attrs = append(attrs, attribute.Int64(attrMessagingKafkaOffset, int64(msg.TopicPartition.Offset)))
	}

	if groupID != "" {
		attrs = append(attrs, attribute.String(attrMessagingKafkaConsumerGroup, groupID))
	}

	spanName := "kafka.consume"
	if msg.TopicPartition.Topic != nil {
		spanName = *msg.TopicPartition.Topic + " receive"
	}

	ctx, span := tracer.Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(attrs...),
	)

	return ctx, span
}

// recordProducerError records an error on a producer span.
func recordProducerError(span trace.Span, err error) {
	if err != nil && span != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}

// recordConsumerError records an error on a consumer span.
func recordConsumerError(span trace.Span, err error) {
	if err != nil && span != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}
