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
	"fmt"
	"strings"
)

// ConfigHint provides configuration suggestions for error remediation
type ConfigHint struct {
	Key            string // Configuration key
	CurrentValue   string // Current value (if known)
	SuggestedValue string // Suggested value
	Docs           string // Link to documentation
}

// EnhancedError wraps a standard Error with additional context and guidance
type EnhancedError struct {
	BaseError  error                  // Original error (maintains compatibility)
	Code       ErrorCode              // Error code
	Message    string                 // Enhanced message
	Context    map[string]interface{} // Additional context
	Causes     []string               // Possible causes
	Solutions  []string               // How to fix
	ConfigHint *ConfigHint            // Configuration suggestion
}

// Error returns a human-readable error message with context
func (e *EnhancedError) Error() string {
	var b strings.Builder

	// Main error message
	b.WriteString(fmt.Sprintf("Error: %s\n", e.Message))

	// Context information
	if len(e.Context) > 0 {
		b.WriteString("\nContext:\n")
		for k, v := range e.Context {
			b.WriteString(fmt.Sprintf("  %s: %v\n", k, v))
		}
	}

	// Possible causes
	if len(e.Causes) > 0 {
		b.WriteString("\nPossible causes:\n")
		for i, cause := range e.Causes {
			b.WriteString(fmt.Sprintf("  %d. %s\n", i+1, cause))
		}
	}

	// Solutions
	if len(e.Solutions) > 0 {
		b.WriteString("\nSolutions:\n")
		for _, sol := range e.Solutions {
			b.WriteString(fmt.Sprintf("  - %s\n", sol))
		}
	}

	// Configuration hints
	if e.ConfigHint != nil {
		b.WriteString("\nConfiguration:\n")
		if e.ConfigHint.CurrentValue != "" {
			b.WriteString(fmt.Sprintf("  Current: %s=%s\n", e.ConfigHint.Key, e.ConfigHint.CurrentValue))
		}
		if e.ConfigHint.SuggestedValue != "" {
			b.WriteString(fmt.Sprintf("  Suggested: %s=%s\n", e.ConfigHint.Key, e.ConfigHint.SuggestedValue))
		}
		if e.ConfigHint.Docs != "" {
			b.WriteString(fmt.Sprintf("  Docs: %s\n", e.ConfigHint.Docs))
		}
	}

	return b.String()
}

// Unwrap returns the base error for errors.Is/As compatibility
func (e *EnhancedError) Unwrap() error {
	return e.BaseError
}

// newQueueFullError creates an enhanced error for queue full conditions
func newQueueFullError(clientType string, queueSize string, currentCount string) *EnhancedError {
	return &EnhancedError{
		Code:    ErrQueueFull,
		Message: fmt.Sprintf("%s queue full (%s/%s messages)", clientType, currentCount, queueSize),
		Context: map[string]interface{}{
			"client_type":   clientType,
			"queue_size":    queueSize,
			"current_count": currentCount,
		},
		Causes: []string{
			"Producing faster than broker can accept",
			"Network issues preventing message delivery",
			"Broker overloaded or unavailable",
			"Queue buffer size too small for workload",
		},
		Solutions: []string{
			"Increase queue buffer size using 'queue.buffering.max.messages' configuration",
			"Implement backpressure: check queue length before producing",
			"Reduce produce rate to match broker capacity",
			"Check broker health and network connectivity",
			"Enable compression to reduce message size ('compression.type')",
		},
		ConfigHint: &ConfigHint{
			Key:            "queue.buffering.max.messages",
			CurrentValue:   queueSize,
			SuggestedValue: "Consider increasing to handle traffic bursts (e.g., 200000-500000)",
			Docs:           "https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md",
		},
	}
}

// newAuthenticationError creates an enhanced error for authentication failures
func newAuthenticationError(broker string, mechanism string) *EnhancedError {
	return &EnhancedError{
		Code:    ErrAuthentication,
		Message: fmt.Sprintf("Authentication failed to broker %s", broker),
		Context: map[string]interface{}{
			"broker":         broker,
			"sasl_mechanism": mechanism,
		},
		Causes: []string{
			"Incorrect username or password",
			"SASL mechanism not supported by broker",
			"SSL/TLS configuration mismatch",
			"Broker access control restrictions",
		},
		Solutions: []string{
			"Verify credentials in 'sasl.username' and 'sasl.password' configuration",
			"Ensure broker supports SASL mechanism: " + mechanism,
			"Review SSL/TLS settings ('security.protocol', 'ssl.ca.location')",
			"Check broker logs for authentication details",
			"Verify network connectivity to broker",
		},
		ConfigHint: &ConfigHint{
			Key:            "sasl.mechanism",
			CurrentValue:   mechanism,
			SuggestedValue: "Supported: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI, OAUTHBEARER",
			Docs:           "https://docs.confluent.io/platform/current/kafka/authentication_sasl/index.html",
		},
	}
}

// newBrokerNotAvailableError creates an enhanced error for broker connectivity issues
func newBrokerNotAvailableError(brokers string) *EnhancedError {
	return &EnhancedError{
		Code:    ErrBrokerNotAvailable,
		Message: "No brokers available for connection",
		Context: map[string]interface{}{
			"bootstrap_servers": brokers,
		},
		Causes: []string{
			"Brokers are down or unreachable",
			"Network connectivity issues",
			"Incorrect broker addresses",
			"Firewall blocking connections",
			"DNS resolution failures",
		},
		Solutions: []string{
			"Verify broker addresses in 'bootstrap.servers' configuration",
			"Check broker status and logs",
			"Test network connectivity: ping/telnet to broker addresses",
			"Verify firewall rules allow connections to Kafka ports (default: 9092)",
			"Check DNS resolution for broker hostnames",
			"Ensure brokers are advertising reachable addresses",
		},
		ConfigHint: &ConfigHint{
			Key:            "bootstrap.servers",
			CurrentValue:   brokers,
			SuggestedValue: "Format: 'host1:port1,host2:port2,host3:port3'",
			Docs:           "https://docs.confluent.io/platform/current/installation/configuration/index.html",
		},
	}
}

// newTopicAuthorizationError creates an enhanced error for authorization failures
func newTopicAuthorizationError(topic string, operation string) *EnhancedError {
	return &EnhancedError{
		Code:    ErrTopicAuthorizationFailed,
		Message: fmt.Sprintf("Not authorized to %s topic '%s'", operation, topic),
		Context: map[string]interface{}{
			"topic":     topic,
			"operation": operation,
		},
		Causes: []string{
			"User lacks required ACL permissions",
			"Topic-level authorization configured on broker",
			"Incorrect service account or credentials",
		},
		Solutions: []string{
			"Request ACL permissions from Kafka administrator",
			fmt.Sprintf("Required permission: %s on topic '%s'", operation, topic),
			"Verify using correct credentials for authorized user",
			"Check broker logs for authorization details",
			"Use kafka-acls tool to verify current permissions",
		},
		ConfigHint: &ConfigHint{
			Key:  "ACL Required",
			Docs: "https://docs.confluent.io/platform/current/kafka/authorization.html",
		},
	}
}

// newRebalanceError creates an enhanced error for consumer group rebalancing issues
func newRebalanceError(groupID string, reason string) *EnhancedError {
	return &EnhancedError{
		Code:    ErrRebalanceInProgress,
		Message: fmt.Sprintf("Consumer group '%s' rebalance in progress: %s", groupID, reason),
		Context: map[string]interface{}{
			"group_id": groupID,
			"reason":   reason,
		},
		Causes: []string{
			"New consumer joined the group",
			"Consumer left or crashed",
			"Session timeout exceeded",
			"Partition count changed for subscribed topics",
		},
		Solutions: []string{
			"Wait for rebalance to complete and retry",
			"Increase 'session.timeout.ms' if consumers timing out",
			"Implement rebalance callback for custom handling",
			"Ensure consumer polls frequently to maintain session",
			"Consider static group membership for stable consumers",
		},
		ConfigHint: &ConfigHint{
			Key:            "session.timeout.ms",
			SuggestedValue: "Default: 45000 (45s). Increase if consumers doing long processing",
			Docs:           "https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html",
		},
	}
}

// newOffsetOutOfRangeError creates an enhanced error for offset range issues
func newOffsetOutOfRangeError(topic string, partition int32, offset int64) *EnhancedError {
	return &EnhancedError{
		Code:    ErrOffsetOutOfRange,
		Message: fmt.Sprintf("Offset %d out of range for topic '%s' partition %d", offset, topic, partition),
		Context: map[string]interface{}{
			"topic":     topic,
			"partition": partition,
			"offset":    offset,
		},
		Causes: []string{
			"Messages have been deleted due to retention policy",
			"Offset was committed but partition data is gone",
			"Partition was recreated or topic deleted",
			"Consumer offset ahead of latest message",
		},
		Solutions: []string{
			"Reset consumer to beginning: 'auto.offset.reset=earliest'",
			"Reset consumer to latest: 'auto.offset.reset=latest'",
			"Manually seek to valid offset using Seek()",
			"Review broker retention settings",
			"Implement offset validation before consuming",
		},
		ConfigHint: &ConfigHint{
			Key:            "auto.offset.reset",
			SuggestedValue: "'earliest' to read from beginning, 'latest' to skip to end",
			Docs:           "https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html#auto.offset.reset",
		},
	}
}

// newMessageTooLargeError creates an enhanced error for message size issues
func newMessageTooLargeError(messageSize int64, maxSize int64) *EnhancedError {
	return &EnhancedError{
		Code:    ErrMsgSizeTooLarge,
		Message: fmt.Sprintf("Message size %d bytes exceeds maximum %d bytes", messageSize, maxSize),
		Context: map[string]interface{}{
			"message_size": messageSize,
			"max_size":     maxSize,
		},
		Causes: []string{
			"Message larger than 'message.max.bytes' broker setting",
			"Message larger than client 'max.request.size' setting",
			"Compression not enabled or ineffective",
		},
		Solutions: []string{
			"Increase client 'message.max.bytes' configuration",
			"Increase broker 'message.max.bytes' setting",
			"Enable compression: 'compression.type=snappy' or 'lz4'",
			"Split large messages into smaller chunks",
			"Store large payloads externally (S3, etc.) and send references",
		},
		ConfigHint: &ConfigHint{
			Key:            "message.max.bytes",
			CurrentValue:   fmt.Sprintf("%d", maxSize),
			SuggestedValue: fmt.Sprintf("%d (increase to accommodate larger messages)", messageSize*2),
			Docs:           "https://docs.confluent.io/platform/current/installation/configuration/topic-configs.html#message.max.bytes",
		},
	}
}

// newSerializationError creates an enhanced error for serialization issues
func newSerializationError(serializerType string, err error) *EnhancedError {
	return &EnhancedError{
		Code:      ErrInvalidArg,
		BaseError: err,
		Message:   fmt.Sprintf("Serialization failed using %s serializer: %v", serializerType, err),
		Context: map[string]interface{}{
			"serializer": serializerType,
		},
		Causes: []string{
			"Data doesn't match schema",
			"Schema not registered in Schema Registry",
			"Schema evolution incompatibility",
			"Invalid data format",
		},
		Solutions: []string{
			"Verify data matches schema definition",
			"Register schema before producing messages",
			"Check schema compatibility settings",
			"Review Schema Registry logs for errors",
			"Validate data before serialization",
		},
		ConfigHint: &ConfigHint{
			Docs: "https://docs.confluent.io/platform/current/schema-registry/index.html",
		},
	}
}

// newNetworkError creates an enhanced error for network-related failures
func newNetworkError(operation string, broker string, err error) *EnhancedError {
	return &EnhancedError{
		Code:      ErrNetworkException,
		BaseError: err,
		Message:   fmt.Sprintf("Network error during %s to broker %s: %v", operation, broker, err),
		Context: map[string]interface{}{
			"operation": operation,
			"broker":    broker,
		},
		Causes: []string{
			"Network connectivity loss",
			"Broker crashed or restarted",
			"Firewall or network policy blocking traffic",
			"Socket timeout exceeded",
			"DNS issues",
		},
		Solutions: []string{
			"Check network connectivity to broker",
			"Verify broker is running and accessible",
			"Review firewall rules and network policies",
			"Increase timeout settings: 'socket.timeout.ms', 'request.timeout.ms'",
			"Check for network instability or packet loss",
			"Review broker logs for connection issues",
		},
		ConfigHint: &ConfigHint{
			Key:            "request.timeout.ms",
			SuggestedValue: "Default: 30000 (30s). Increase for unstable networks",
			Docs:           "https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md",
		},
	}
}
