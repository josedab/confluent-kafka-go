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
	"io"
	"log/slog"
	"os"
	"sync"
)

// Logger is the interface for structured logging in confluent-kafka-go.
// Applications can provide custom implementations or use the default slog-based logger.
//
// Example usage:
//
//	// Use JSON logging
//	logger := kafka.NewJSONLogger(os.Stdout, slog.LevelInfo)
//	kafka.SetLogger(logger)
//
//	// Use text logging
//	logger := kafka.NewTextLogger(os.Stderr, slog.LevelDebug)
//	kafka.SetLogger(logger)
//
//	// Use with additional context
//	loggerWithContext := kafka.GetLogger().With(
//	    "environment", "production",
//	    "service", "order-processor",
//	)
type Logger interface {
	// Debug logs a debug-level message with context and attributes.
	// Attributes should be provided as key-value pairs.
	//
	// Example:
	//   logger.Debug(ctx, "Processing message",
	//       "topic", "orders",
	//       "partition", 0,
	//       "offset", 12345)
	Debug(ctx context.Context, msg string, args ...any)

	// Info logs an info-level message with context and attributes.
	Info(ctx context.Context, msg string, args ...any)

	// Warn logs a warning-level message with context and attributes.
	Warn(ctx context.Context, msg string, args ...any)

	// Error logs an error-level message with context and attributes.
	Error(ctx context.Context, msg string, args ...any)

	// With returns a new Logger with additional attributes.
	// The returned logger will include these attributes in all subsequent log calls.
	//
	// Example:
	//   producerLogger := logger.With("client_type", "producer", "client_id", "producer-1")
	//   producerLogger.Info(ctx, "Started") // Includes client_type and client_id
	With(args ...any) Logger

	// Enabled reports whether the logger handles records at the given level.
	Enabled(ctx context.Context, level slog.Level) bool
}

// SlogLogger wraps slog.Logger to implement the Logger interface.
type SlogLogger struct {
	logger *slog.Logger
}

// NewSlogLogger creates a new structured logger using a custom slog handler.
//
// Example:
//
//	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
//	    Level: slog.LevelDebug,
//	})
//	logger := kafka.NewSlogLogger(handler)
func NewSlogLogger(handler slog.Handler) Logger {
	return &SlogLogger{
		logger: slog.New(handler),
	}
}

// NewJSONLogger creates a JSON-formatted logger.
//
// Example:
//
//	logger := kafka.NewJSONLogger(os.Stdout, slog.LevelInfo)
//	kafka.SetLogger(logger)
func NewJSONLogger(w io.Writer, level slog.Level) Logger {
	handler := slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level: level,
	})
	return NewSlogLogger(handler)
}

// NewTextLogger creates a human-readable text logger.
//
// Example:
//
//	logger := kafka.NewTextLogger(os.Stderr, slog.LevelDebug)
//	kafka.SetLogger(logger)
func NewTextLogger(w io.Writer, level slog.Level) Logger {
	handler := slog.NewTextHandler(w, &slog.HandlerOptions{
		Level: level,
	})
	return NewSlogLogger(handler)
}

// Debug logs a debug-level message
func (l *SlogLogger) Debug(ctx context.Context, msg string, args ...any) {
	l.logger.DebugContext(ctx, msg, args...)
}

// Info logs an info-level message
func (l *SlogLogger) Info(ctx context.Context, msg string, args ...any) {
	l.logger.InfoContext(ctx, msg, args...)
}

// Warn logs a warning-level message
func (l *SlogLogger) Warn(ctx context.Context, msg string, args ...any) {
	l.logger.WarnContext(ctx, msg, args...)
}

// Error logs an error-level message
func (l *SlogLogger) Error(ctx context.Context, msg string, args ...any) {
	l.logger.ErrorContext(ctx, msg, args...)
}

// With returns a new Logger with additional attributes
func (l *SlogLogger) With(args ...any) Logger {
	return &SlogLogger{
		logger: l.logger.With(args...),
	}
}

// Enabled reports whether the logger handles records at the given level
func (l *SlogLogger) Enabled(ctx context.Context, level slog.Level) bool {
	return l.logger.Enabled(ctx, level)
}

// Default logger instance
var (
	defaultLogger   Logger
	defaultLoggerMu sync.RWMutex
)

func init() {
	// Initialize with JSON logger to stderr at Info level by default
	defaultLogger = NewJSONLogger(os.Stderr, slog.LevelInfo)
}

// SetLogger sets the global logger for confluent-kafka-go.
// All producers, consumers, and admin clients created after this call will use the new logger.
//
// Example:
//
//	logger := kafka.NewJSONLogger(os.Stdout, slog.LevelDebug)
//	kafka.SetLogger(logger)
func SetLogger(logger Logger) {
	defaultLoggerMu.Lock()
	defer defaultLoggerMu.Unlock()
	defaultLogger = logger
}

// GetLogger returns the current global logger.
//
// Example:
//
//	logger := kafka.GetLogger()
//	customLogger := logger.With("service", "my-app")
func GetLogger() Logger {
	defaultLoggerMu.RLock()
	defer defaultLoggerMu.RUnlock()
	return defaultLogger
}

// ParseLogLevel converts a string log level to slog.Level.
//
// Supported values: "debug", "info", "warn", "error"
// Returns slog.LevelInfo for invalid inputs.
func ParseLogLevel(level string) slog.Level {
	switch level {
	case "debug", "DEBUG":
		return slog.LevelDebug
	case "info", "INFO":
		return slog.LevelInfo
	case "warn", "WARN", "warning", "WARNING":
		return slog.LevelWarn
	case "error", "ERROR":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// mapLibrdkafkaLevel maps librdkafka's syslog-style levels (0-7) to slog levels.
//
// librdkafka log levels (from highest to lowest priority):
// 0 = LOG_EMERG   (system is unusable)
// 1 = LOG_ALERT   (action must be taken immediately)
// 2 = LOG_CRIT    (critical conditions)
// 3 = LOG_ERR     (error conditions)
// 4 = LOG_WARNING (warning conditions)
// 5 = LOG_NOTICE  (normal but significant condition)
// 6 = LOG_INFO    (informational)
// 7 = LOG_DEBUG   (debug-level messages)
func mapLibrdkafkaLevel(level int) slog.Level {
	switch {
	case level <= 3: // Emergency, Alert, Critical, Error
		return slog.LevelError
	case level == 4: // Warning
		return slog.LevelWarn
	case level <= 6: // Notice, Informational
		return slog.LevelInfo
	default: // Debug (7)
		return slog.LevelDebug
	}
}

// NoOpLogger is a logger that discards all log messages.
// Useful for testing or when logging is not desired.
type NoOpLogger struct{}

// NewNoOpLogger creates a logger that discards all messages
func NewNoOpLogger() Logger {
	return &NoOpLogger{}
}

func (l *NoOpLogger) Debug(ctx context.Context, msg string, args ...any) {}
func (l *NoOpLogger) Info(ctx context.Context, msg string, args ...any)  {}
func (l *NoOpLogger) Warn(ctx context.Context, msg string, args ...any)  {}
func (l *NoOpLogger) Error(ctx context.Context, msg string, args ...any) {}
func (l *NoOpLogger) With(args ...any) Logger                            { return l }
func (l *NoOpLogger) Enabled(ctx context.Context, level slog.Level) bool { return false }
