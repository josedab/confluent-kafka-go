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
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

func TestJSONLogger(t *testing.T) {
	var buf bytes.Buffer
	logger := NewJSONLogger(&buf, slog.LevelDebug)

	ctx := context.Background()
	logger.Info(ctx, "test message", "key1", "value1", "key2", 42)

	output := buf.String()
	if !strings.Contains(output, "test message") {
		t.Errorf("Log output missing message: %s", output)
	}
	if !strings.Contains(output, "key1") {
		t.Errorf("Log output missing key1: %s", output)
	}
	if !strings.Contains(output, "value1") {
		t.Errorf("Log output missing value1: %s", output)
	}
}

func TestTextLogger(t *testing.T) {
	var buf bytes.Buffer
	logger := NewTextLogger(&buf, slog.LevelInfo)

	ctx := context.Background()
	logger.Info(ctx, "test message", "topic", "orders")

	output := buf.String()
	if !strings.Contains(output, "test message") {
		t.Errorf("Log output missing message: %s", output)
	}
	if !strings.Contains(output, "topic") {
		t.Errorf("Log output missing topic key: %s", output)
	}
}

func TestLoggerWith(t *testing.T) {
	var buf bytes.Buffer
	logger := NewJSONLogger(&buf, slog.LevelDebug)

	// Create logger with additional attributes
	producerLogger := logger.With("client_type", "producer", "client_id", "producer-1")

	ctx := context.Background()
	producerLogger.Info(ctx, "message sent", "topic", "orders")

	output := buf.String()
	if !strings.Contains(output, "producer") {
		t.Errorf("Log output missing client_type: %s", output)
	}
	if !strings.Contains(output, "producer-1") {
		t.Errorf("Log output missing client_id: %s", output)
	}
	if !strings.Contains(output, "message sent") {
		t.Errorf("Log output missing message: %s", output)
	}
}

func TestLogLevels(t *testing.T) {
	var buf bytes.Buffer
	logger := NewJSONLogger(&buf, slog.LevelWarn)

	ctx := context.Background()

	// Debug and Info should not appear (level is Warn)
	logger.Debug(ctx, "debug message")
	logger.Info(ctx, "info message")

	if strings.Contains(buf.String(), "debug message") {
		t.Errorf("Debug message should not appear at Warn level")
	}
	if strings.Contains(buf.String(), "info message") {
		t.Errorf("Info message should not appear at Warn level")
	}

	// Warn and Error should appear
	logger.Warn(ctx, "warn message")
	logger.Error(ctx, "error message")

	output := buf.String()
	if !strings.Contains(output, "warn message") {
		t.Errorf("Warn message should appear: %s", output)
	}
	if !strings.Contains(output, "error message") {
		t.Errorf("Error message should appear: %s", output)
	}
}

func TestSetGetLogger(t *testing.T) {
	// Save original logger
	originalLogger := GetLogger()
	defer SetLogger(originalLogger)

	// Create and set new logger
	var buf bytes.Buffer
	newLogger := NewJSONLogger(&buf, slog.LevelDebug)
	SetLogger(newLogger)

	// Verify logger was set
	if GetLogger() != newLogger {
		t.Errorf("GetLogger did not return the set logger")
	}

	// Verify it works
	ctx := context.Background()
	GetLogger().Info(ctx, "test")

	if !strings.Contains(buf.String(), "test") {
		t.Errorf("Set logger did not log message")
	}
}

func TestParseLogLevel(t *testing.T) {
	tests := []struct {
		input    string
		expected slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"DEBUG", slog.LevelDebug},
		{"info", slog.LevelInfo},
		{"INFO", slog.LevelInfo},
		{"warn", slog.LevelWarn},
		{"WARN", slog.LevelWarn},
		{"warning", slog.LevelWarn},
		{"error", slog.LevelError},
		{"ERROR", slog.LevelError},
		{"invalid", slog.LevelInfo}, // Default to Info
		{"", slog.LevelInfo},         // Default to Info
	}

	for _, tt := range tests {
		result := ParseLogLevel(tt.input)
		if result != tt.expected {
			t.Errorf("ParseLogLevel(%q) = %v, expected %v", tt.input, result, tt.expected)
		}
	}
}

func TestMapLibrdkafkaLevel(t *testing.T) {
	tests := []struct {
		librdkafkaLevel int
		expectedLevel   slog.Level
	}{
		{0, slog.LevelError},  // Emergency
		{1, slog.LevelError},  // Alert
		{2, slog.LevelError},  // Critical
		{3, slog.LevelError},  // Error
		{4, slog.LevelWarn},   // Warning
		{5, slog.LevelInfo},   // Notice
		{6, slog.LevelInfo},   // Informational
		{7, slog.LevelDebug},  // Debug
		{8, slog.LevelDebug},  // Higher values default to Debug
	}

	for _, tt := range tests {
		result := mapLibrdkafkaLevel(tt.librdkafkaLevel)
		if result != tt.expectedLevel {
			t.Errorf("mapLibrdkafkaLevel(%d) = %v, expected %v",
				tt.librdkafkaLevel, result, tt.expectedLevel)
		}
	}
}

func TestNoOpLogger(t *testing.T) {
	logger := NewNoOpLogger()
	ctx := context.Background()

	// Should not panic
	logger.Debug(ctx, "test")
	logger.Info(ctx, "test")
	logger.Warn(ctx, "test")
	logger.Error(ctx, "test")

	// With should return self
	withLogger := logger.With("key", "value")
	if withLogger != logger {
		t.Errorf("NoOpLogger.With() should return self")
	}

	// Enabled should always be false
	if logger.Enabled(ctx, slog.LevelError) {
		t.Errorf("NoOpLogger should never be enabled")
	}
}

func TestLoggerEnabled(t *testing.T) {
	var buf bytes.Buffer
	logger := NewJSONLogger(&buf, slog.LevelWarn)

	ctx := context.Background()

	if logger.Enabled(ctx, slog.LevelDebug) {
		t.Errorf("Logger should not be enabled for Debug when level is Warn")
	}
	if logger.Enabled(ctx, slog.LevelInfo) {
		t.Errorf("Logger should not be enabled for Info when level is Warn")
	}
	if !logger.Enabled(ctx, slog.LevelWarn) {
		t.Errorf("Logger should be enabled for Warn when level is Warn")
	}
	if !logger.Enabled(ctx, slog.LevelError) {
		t.Errorf("Logger should be enabled for Error when level is Warn")
	}
}

// BenchmarkJSONLogger measures JSON logging performance
func BenchmarkJSONLogger(b *testing.B) {
	var buf bytes.Buffer
	logger := NewJSONLogger(&buf, slog.LevelInfo)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.Info(ctx, "benchmark message",
			"iteration", i,
			"topic", "orders",
			"partition", 0,
			"offset", int64(i*1000))
	}
}

// BenchmarkTextLogger measures text logging performance
func BenchmarkTextLogger(b *testing.B) {
	var buf bytes.Buffer
	logger := NewTextLogger(&buf, slog.LevelInfo)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.Info(ctx, "benchmark message",
			"iteration", i,
			"topic", "orders")
	}
}

// BenchmarkLoggerWith measures performance of With()
func BenchmarkLoggerWith(b *testing.B) {
	var buf bytes.Buffer
	logger := NewJSONLogger(&buf, slog.LevelInfo)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		withLogger := logger.With("client_id", "producer-1", "app", "test")
		withLogger.Info(ctx, "message", "count", i)
	}
}
