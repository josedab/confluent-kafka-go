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

// Structured logging example for confluent-kafka-go
//
// This example demonstrates how to use the structured logging feature
// to get machine-parseable JSON logs with rich context.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	fmt.Println("=== Structured Logging Examples ===\n")

	// Example 1: JSON Logger (default, recommended for production)
	fmt.Println("Example 1: JSON Logger")
	fmt.Println("----------------------")
	jsonExample()
	fmt.Println()

	// Example 2: Text Logger (human-readable, good for development)
	fmt.Println("\nExample 2: Text Logger")
	fmt.Println("----------------------")
	textExample()
	fmt.Println()

	// Example 3: Logger with Context
	fmt.Println("\nExample 3: Logger with Additional Context")
	fmt.Println("-----------------------------------------")
	contextExample()
	fmt.Println()

	// Example 4: Custom Log Levels
	fmt.Println("\nExample 4: Custom Log Levels")
	fmt.Println("----------------------------")
	logLevelExample()
	fmt.Println()

	// Example 5: Application Integration
	fmt.Println("\nExample 5: Producer with Structured Logging")
	fmt.Println("-------------------------------------------")
	producerExample()
}

func jsonExample() {
	// Create JSON logger that outputs to stdout at Info level
	logger := kafka.NewJSONLogger(os.Stdout, slog.LevelInfo)
	kafka.SetLogger(logger)

	ctx := context.Background()

	// Log messages will be in JSON format
	logger.Info(ctx, "Application started",
		"version", "1.0.0",
		"environment", "production")

	logger.Info(ctx, "Kafka client initialized",
		"bootstrap_servers", "localhost:9092",
		"group_id", "my-consumer-group")

	logger.Error(ctx, "Failed to connect to broker",
		"broker", "localhost:9092",
		"error", "connection refused",
		"retry_count", 3)
}

func textExample() {
	// Create text logger for human-readable output
	logger := kafka.NewTextLogger(os.Stdout, slog.LevelDebug)
	kafka.SetLogger(logger)

	ctx := context.Background()

	// Log messages will be in text format
	logger.Debug(ctx, "Detailed debug information",
		"function", "processMessage",
		"duration_ms", 42)

	logger.Info(ctx, "Processing message",
		"topic", "orders",
		"partition", 0,
		"offset", 12345)

	logger.Warn(ctx, "High lag detected",
		"topic", "orders",
		"partition", 0,
		"lag", 10000)
}

func contextExample() {
	// Create base logger
	baseLogger := kafka.NewJSONLogger(os.Stdout, slog.LevelInfo)
	kafka.SetLogger(baseLogger)

	// Create logger with service-level context
	serviceLogger := baseLogger.With(
		"service", "order-processor",
		"version", "2.1.0",
		"environment", "production",
	)

	// Create logger with request-specific context
	requestLogger := serviceLogger.With(
		"request_id", "req-123456",
		"customer_id", "cust-789",
	)

	ctx := context.Background()

	// All these attributes will be included in every log
	requestLogger.Info(ctx, "Order received")
	requestLogger.Info(ctx, "Order validated", "order_id", "ord-001")
	requestLogger.Info(ctx, "Order processed", "duration_ms", 150)
}

func logLevelExample() {
	// Parse log level from environment or config
	levelStr := os.Getenv("LOG_LEVEL")
	if levelStr == "" {
		levelStr = "info"
	}
	level := kafka.ParseLogLevel(levelStr)

	logger := kafka.NewJSONLogger(os.Stdout, level)
	kafka.SetLogger(logger)

	ctx := context.Background()

	// These will only appear if level is Debug
	logger.Debug(ctx, "Verbose debugging information",
		"bytes_processed", 1024)

	// These appear at Info level and above
	logger.Info(ctx, "Normal operational message",
		"status", "healthy")

	// These appear at Warn level and above
	logger.Warn(ctx, "Something unusual happened",
		"warning_type", "high_memory_usage")

	// These always appear (unless logging is disabled)
	logger.Error(ctx, "Something went wrong",
		"error", "timeout")
}

func producerExample() {
	// Set up structured logging for the application
	logger := kafka.NewJSONLogger(os.Stdout, slog.LevelInfo)
	kafka.SetLogger(logger)

	ctx := context.Background()
	logger.Info(ctx, "Creating Kafka producer")

	// Create producer
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"client.id":         "structured-logging-example",
	})

	if err != nil {
		logger.Error(ctx, "Failed to create producer",
			"error", err.Error())
		return
	}
	defer p.Close()

	// Create producer-specific logger with context
	producerLogger := logger.With(
		"client_type", "producer",
		"client_id", "structured-logging-example",
	)

	producerLogger.Info(ctx, "Producer created successfully")

	// Produce a message
	topic := "test-topic"
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte("test message"),
	}

	producerLogger.Info(ctx, "Producing message",
		"topic", topic,
		"value_size", len(msg.Value))

	err = p.Produce(msg, nil)
	if err != nil {
		producerLogger.Error(ctx, "Failed to produce message",
			"error", err.Error(),
			"topic", topic)
	} else {
		producerLogger.Info(ctx, "Message queued successfully",
			"topic", topic)
	}

	// Flush messages
	remaining := p.Flush(5000)
	if remaining > 0 {
		producerLogger.Warn(ctx, "Messages still in queue after flush",
			"remaining", remaining)
	} else {
		producerLogger.Info(ctx, "All messages flushed successfully")
	}
}

// Example output from JSON logger:
//
// {"time":"2025-11-16T10:15:23Z","level":"INFO","msg":"Application started","version":"1.0.0","environment":"production"}
// {"time":"2025-11-16T10:15:23Z","level":"INFO","msg":"Kafka client initialized","bootstrap_servers":"localhost:9092","group_id":"my-consumer-group"}
// {"time":"2025-11-16T10:15:23Z","level":"ERROR","msg":"Failed to connect to broker","broker":"localhost:9092","error":"connection refused","retry_count":3}
//
// Example output from text logger:
//
// time=2025-11-16T10:15:23.000Z level=DEBUG msg="Detailed debug information" function=processMessage duration_ms=42
// time=2025-11-16T10:15:23.000Z level=INFO msg="Processing message" topic=orders partition=0 offset=12345
// time=2025-11-16T10:15:23.000Z level=WARN msg="High lag detected" topic=orders partition=0 lag=10000
