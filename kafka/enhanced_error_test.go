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
	"errors"
	"strings"
	"testing"
)

func TestEnhancedError_Error(t *testing.T) {
	err := &EnhancedError{
		Code:    ErrQueueFull,
		Message: "Producer queue full",
		Context: map[string]interface{}{
			"queue_size": "100000",
			"client":     "producer-1",
		},
		Causes: []string{
			"Producing too fast",
			"Network issues",
		},
		Solutions: []string{
			"Increase queue size",
			"Add backpressure",
		},
		ConfigHint: &ConfigHint{
			Key:            "queue.buffering.max.messages",
			CurrentValue:   "100000",
			SuggestedValue: "200000",
			Docs:           "https://docs.example.com",
		},
	}

	errStr := err.Error()

	// Verify all sections are present
	if !strings.Contains(errStr, "Producer queue full") {
		t.Errorf("Error message missing main message")
	}
	if !strings.Contains(errStr, "Context:") {
		t.Errorf("Error message missing context section")
	}
	if !strings.Contains(errStr, "Possible causes:") {
		t.Errorf("Error message missing causes section")
	}
	if !strings.Contains(errStr, "Solutions:") {
		t.Errorf("Error message missing solutions section")
	}
	if !strings.Contains(errStr, "Configuration:") {
		t.Errorf("Error message missing configuration section")
	}
}

func TestEnhancedError_Unwrap(t *testing.T) {
	baseErr := errors.New("base error")
	enhanced := &EnhancedError{
		BaseError: baseErr,
		Code:      ErrInvalidArg,
		Message:   "Enhanced message",
	}

	unwrapped := errors.Unwrap(enhanced)
	if unwrapped != baseErr {
		t.Errorf("Unwrap failed: expected %v, got %v", baseErr, unwrapped)
	}
}

func TestNewQueueFullError(t *testing.T) {
	err := newQueueFullError("producer", "100000", "100000")

	if err.Code != ErrQueueFull {
		t.Errorf("Expected code ErrQueueFull, got %v", err.Code)
	}

	errStr := err.Error()
	if !strings.Contains(errStr, "producer queue full") {
		t.Errorf("Error message doesn't mention producer: %s", errStr)
	}
	if !strings.Contains(errStr, "queue.buffering.max.messages") {
		t.Errorf("Error message doesn't mention queue config: %s", errStr)
	}
}

func TestNewAuthenticationError(t *testing.T) {
	err := newAuthenticationError("broker1:9092", "SCRAM-SHA-256")

	if err.Code != ErrAuthentication {
		t.Errorf("Expected code ErrAuthentication, got %v", err.Code)
	}

	errStr := err.Error()
	if !strings.Contains(errStr, "broker1:9092") {
		t.Errorf("Error message doesn't mention broker: %s", errStr)
	}
	if !strings.Contains(errStr, "SCRAM-SHA-256") {
		t.Errorf("Error message doesn't mention SASL mechanism: %s", errStr)
	}
}

func TestNewBrokerNotAvailableError(t *testing.T) {
	err := newBrokerNotAvailableError("localhost:9092")

	if err.Code != ErrBrokerNotAvailable {
		t.Errorf("Expected code ErrBrokerNotAvailable, got %v", err.Code)
	}

	errStr := err.Error()
	if !strings.Contains(errStr, "bootstrap.servers") {
		t.Errorf("Error message doesn't mention bootstrap servers: %s", errStr)
	}
	if !strings.Contains(errStr, "localhost:9092") {
		t.Errorf("Error message doesn't mention broker address: %s", errStr)
	}
}

func TestNewTopicAuthorizationError(t *testing.T) {
	err := newTopicAuthorizationError("test-topic", "WRITE")

	if err.Code != ErrTopicAuthorizationFailed {
		t.Errorf("Expected code ErrTopicAuthorizationFailed, got %v", err.Code)
	}

	errStr := err.Error()
	if !strings.Contains(errStr, "test-topic") {
		t.Errorf("Error message doesn't mention topic: %s", errStr)
	}
	if !strings.Contains(errStr, "WRITE") {
		t.Errorf("Error message doesn't mention operation: %s", errStr)
	}
}

func TestNewRebalanceError(t *testing.T) {
	err := newRebalanceError("my-consumer-group", "new member joined")

	if err.Code != ErrRebalanceInProgress {
		t.Errorf("Expected code ErrRebalanceInProgress, got %v", err.Code)
	}

	errStr := err.Error()
	if !strings.Contains(errStr, "my-consumer-group") {
		t.Errorf("Error message doesn't mention group: %s", errStr)
	}
	if !strings.Contains(errStr, "session.timeout.ms") {
		t.Errorf("Error message doesn't mention timeout config: %s", errStr)
	}
}

func TestNewOffsetOutOfRangeError(t *testing.T) {
	err := newOffsetOutOfRangeError("test-topic", 0, 12345)

	if err.Code != ErrOffsetOutOfRange {
		t.Errorf("Expected code ErrOffsetOutOfRange, got %v", err.Code)
	}

	errStr := err.Error()
	if !strings.Contains(errStr, "test-topic") {
		t.Errorf("Error message doesn't mention topic: %s", errStr)
	}
	if !strings.Contains(errStr, "auto.offset.reset") {
		t.Errorf("Error message doesn't mention offset reset config: %s", errStr)
	}
}

func TestNewMessageTooLargeError(t *testing.T) {
	err := newMessageTooLargeError(2000000, 1000000)

	if err.Code != ErrMsgSizeTooLarge {
		t.Errorf("Expected code ErrMsgSizeTooLarge, got %v", err.Code)
	}

	errStr := err.Error()
	if !strings.Contains(errStr, "2000000") {
		t.Errorf("Error message doesn't mention message size: %s", errStr)
	}
	if !strings.Contains(errStr, "message.max.bytes") {
		t.Errorf("Error message doesn't mention size config: %s", errStr)
	}
}

func TestNewSerializationError(t *testing.T) {
	baseErr := errors.New("schema mismatch")
	err := newSerializationError("Avro", baseErr)

	if err.Code != ErrInvalidArg {
		t.Errorf("Expected code ErrInvalidArg, got %v", err.Code)
	}

	errStr := err.Error()
	if !strings.Contains(errStr, "Avro") {
		t.Errorf("Error message doesn't mention serializer: %s", errStr)
	}
	if !strings.Contains(errStr, "schema mismatch") {
		t.Errorf("Error message doesn't mention base error: %s", errStr)
	}
}

func TestNewNetworkError(t *testing.T) {
	baseErr := errors.New("connection refused")
	err := newNetworkError("produce", "broker1:9092", baseErr)

	if err.Code != ErrNetworkException {
		t.Errorf("Expected code ErrNetworkException, got %v", err.Code)
	}

	errStr := err.Error()
	if !strings.Contains(errStr, "produce") {
		t.Errorf("Error message doesn't mention operation: %s", errStr)
	}
	if !strings.Contains(errStr, "broker1:9092") {
		t.Errorf("Error message doesn't mention broker: %s", errStr)
	}
	if !strings.Contains(errStr, "request.timeout.ms") {
		t.Errorf("Error message doesn't mention timeout config: %s", errStr)
	}
}
