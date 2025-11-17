/**
 * Copyright 2024 Confluent Inc.
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

import "sync"

// MessagePool provides object pooling for Message structs to reduce GC pressure
// in high-throughput scenarios (>100K msg/sec).
//
// This is an opt-in performance optimization that can be enabled via the
// "go.message.pool.enable" configuration option when creating a Producer or Consumer.
type MessagePool struct {
	pool sync.Pool
}

// NewMessagePool creates a new message pool.
func NewMessagePool() *MessagePool {
	return &MessagePool{
		pool: sync.Pool{
			New: func() interface{} {
				return &Message{}
			},
		},
	}
}

// Get retrieves a message from the pool.
// The returned message should be returned to the pool via Put() when no longer needed.
func (p *MessagePool) Get() *Message {
	return p.pool.Get().(*Message)
}

// Put returns a message to the pool after clearing sensitive data.
// This prevents memory leaks by ensuring references are cleared before pooling.
func (p *MessagePool) Put(msg *Message) {
	if msg == nil {
		return
	}

	// Clear all references to prevent memory leaks
	msg.Key = nil
	msg.Value = nil
	msg.Headers = nil
	msg.TopicPartition.Topic = nil
	msg.TopicPartition.Error = nil
	msg.TopicPartition.LeaderEpoch = nil
	msg.Opaque = nil
	msg.LeaderEpoch = nil

	// Reset other fields to zero values
	msg.TopicPartition.Partition = 0
	msg.TopicPartition.Offset = 0
	msg.TimestampType = TimestampNotAvailable

	p.pool.Put(msg)
}
