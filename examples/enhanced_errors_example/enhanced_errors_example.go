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

// Enhanced error handling example
// This example demonstrates how the enhanced error system provides actionable
// guidance when errors occur.
package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	fmt.Println("=== Enhanced Error Handling Example ===\n")

	// Example 1: Queue Full Error
	fmt.Println("Example 1: Queue Full Error")
	fmt.Println("---------------------------")
	queueErr := demonstrateQueueFullError()
	fmt.Println(queueErr.Error())
	fmt.Println()

	// Example 2: Authentication Error
	fmt.Println("Example 2: Authentication Error")
	fmt.Println("-------------------------------")
	authErr := demonstrateAuthenticationError()
	fmt.Println(authErr.Error())
	fmt.Println()

	// Example 3: Broker Not Available Error
	fmt.Println("Example 3: Broker Not Available")
	fmt.Println("-------------------------------")
	brokerErr := demonstrateBrokerError()
	fmt.Println(brokerErr.Error())
	fmt.Println()

	// Example 4: Topic Authorization Error
	fmt.Println("Example 4: Topic Authorization Error")
	fmt.Println("------------------------------------")
	authzErr := demonstrateAuthorizationError()
	fmt.Println(authzErr.Error())
	fmt.Println()

	// Example 5: Offset Out of Range Error
	fmt.Println("Example 5: Offset Out of Range")
	fmt.Println("------------------------------")
	offsetErr := demonstrateOffsetError()
	fmt.Println(offsetErr.Error())
	fmt.Println()

	// Example 6: Message Too Large Error
	fmt.Println("Example 6: Message Too Large")
	fmt.Println("----------------------------")
	sizeErr := demonstrateMessageSizeError()
	fmt.Println(sizeErr.Error())
	fmt.Println()
}

func demonstrateQueueFullError() *kafka.EnhancedError {
	// This simulates a producer queue full scenario
	return &kafka.EnhancedError{
		Code:    kafka.ErrQueueFull,
		Message: "Producer queue full (100000/100000 messages)",
		Context: map[string]interface{}{
			"client_type":   "producer",
			"queue_size":    "100000",
			"current_count": "100000",
		},
		Causes: []string{
			"Producing faster than broker can accept",
			"Network issues preventing message delivery",
			"Broker overloaded or unavailable",
		},
		Solutions: []string{
			"Increase queue.buffering.max.messages",
			"Implement backpressure handling",
			"Check broker health",
		},
		ConfigHint: &kafka.ConfigHint{
			Key:            "queue.buffering.max.messages",
			CurrentValue:   "100000",
			SuggestedValue: "200000",
			Docs:           "https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md",
		},
	}
}

func demonstrateAuthenticationError() *kafka.EnhancedError{
	return &kafka.EnhancedError{
		Code:    kafka.ErrAuthentication,
		Message: "Authentication failed to broker kafka1:9093",
		Context: map[string]interface{}{
			"broker":         "kafka1:9093",
			"sasl_mechanism": "SCRAM-SHA-256",
		},
		Causes: []string{
			"Incorrect username or password",
			"SASL mechanism not supported by broker",
			"SSL/TLS configuration mismatch",
		},
		Solutions: []string{
			"Verify sasl.username and sasl.password",
			"Check broker supports SCRAM-SHA-256",
			"Review SSL/TLS settings",
		},
		ConfigHint: &kafka.ConfigHint{
			Key:          "sasl.mechanism",
			CurrentValue: "SCRAM-SHA-256",
			Docs:         "https://docs.confluent.io/platform/current/kafka/authentication_sasl/index.html",
		},
	}
}

func demonstrateBrokerError() *kafka.EnhancedError {
	return &kafka.EnhancedError{
		Code:    kafka.ErrBrokerNotAvailable,
		Message: "No brokers available for connection",
		Context: map[string]interface{}{
			"bootstrap_servers": "localhost:9092",
		},
		Causes: []string{
			"Brokers are down or unreachable",
			"Network connectivity issues",
			"Incorrect broker addresses",
		},
		Solutions: []string{
			"Verify broker addresses",
			"Check broker status",
			"Test network connectivity",
		},
		ConfigHint: &kafka.ConfigHint{
			Key:          "bootstrap.servers",
			CurrentValue: "localhost:9092",
			Docs:         "https://docs.confluent.io/platform/current/installation/configuration/index.html",
		},
	}
}

func demonstrateAuthorizationError() *kafka.EnhancedError {
	return &kafka.EnhancedError{
		Code:    kafka.ErrTopicAuthorizationFailed,
		Message: "Not authorized to WRITE topic 'secure-topic'",
		Context: map[string]interface{}{
			"topic":     "secure-topic",
			"operation": "WRITE",
		},
		Causes: []string{
			"User lacks required ACL permissions",
			"Topic-level authorization configured",
		},
		Solutions: []string{
			"Request ACL permissions from administrator",
			"Required: WRITE permission on 'secure-topic'",
			"Verify using correct credentials",
		},
		ConfigHint: &kafka.ConfigHint{
			Docs: "https://docs.confluent.io/platform/current/kafka/authorization.html",
		},
	}
}

func demonstrateOffsetError() *kafka.EnhancedError {
	return &kafka.EnhancedError{
		Code:    kafka.ErrOffsetOutOfRange,
		Message: "Offset 12345 out of range for topic 'events' partition 0",
		Context: map[string]interface{}{
			"topic":     "events",
			"partition": 0,
			"offset":    12345,
		},
		Causes: []string{
			"Messages deleted due to retention policy",
			"Partition was recreated",
		},
		Solutions: []string{
			"Reset to beginning: auto.offset.reset=earliest",
			"Reset to latest: auto.offset.reset=latest",
			"Manually seek to valid offset",
		},
		ConfigHint: &kafka.ConfigHint{
			Key:            "auto.offset.reset",
			SuggestedValue: "earliest or latest",
			Docs:           "https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html",
		},
	}
}

func demonstrateMessageSizeError() *kafka.EnhancedError {
	return &kafka.EnhancedError{
		Code:    kafka.ErrMsgSizeTooLarge,
		Message: "Message size 2000000 bytes exceeds maximum 1000000 bytes",
		Context: map[string]interface{}{
			"message_size": 2000000,
			"max_size":     1000000,
		},
		Causes: []string{
			"Message larger than message.max.bytes",
			"Compression not enabled",
		},
		Solutions: []string{
			"Increase message.max.bytes",
			"Enable compression (snappy, lz4)",
			"Split large messages",
		},
		ConfigHint: &kafka.ConfigHint{
			Key:            "message.max.bytes",
			CurrentValue:   "1000000",
			SuggestedValue: "4000000",
			Docs:           "https://docs.confluent.io/platform/current/installation/configuration/topic-configs.html",
		},
	}
}
