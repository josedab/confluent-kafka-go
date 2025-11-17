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

import (
	"testing"
)

// TestMessagePoolBasic tests basic pool functionality
func TestMessagePoolBasic(t *testing.T) {
	pool := NewMessagePool()

	// Get a message from the pool
	msg1 := pool.Get()
	if msg1 == nil {
		t.Fatal("Expected non-nil message from pool")
	}

	// Set some values
	topic := "test-topic"
	msg1.TopicPartition.Topic = &topic
	msg1.TopicPartition.Partition = 1
	msg1.Value = []byte("test value")
	msg1.Key = []byte("test key")

	// Return to pool
	pool.Put(msg1)

	// Get another message - might be the same one
	msg2 := pool.Get()
	if msg2 == nil {
		t.Fatal("Expected non-nil message from pool")
	}

	// Verify fields were cleared
	if msg2.TopicPartition.Topic != nil {
		t.Error("Expected Topic to be nil after pool Put")
	}
	if msg2.Value != nil {
		t.Error("Expected Value to be nil after pool Put")
	}
	if msg2.Key != nil {
		t.Error("Expected Key to be nil after pool Put")
	}
	if msg2.TopicPartition.Partition != 0 {
		t.Error("Expected Partition to be 0 after pool Put")
	}
}

// TestMessagePoolNilPut tests that putting nil doesn't panic
func TestMessagePoolNilPut(t *testing.T) {
	pool := NewMessagePool()
	pool.Put(nil) // Should not panic
}

// TestMessagePoolConcurrency tests concurrent access to pool
func TestMessagePoolConcurrency(t *testing.T) {
	pool := NewMessagePool()
	iterations := 1000

	// Channel to signal completion
	done := make(chan bool)

	// Spawn multiple goroutines
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < iterations; j++ {
				msg := pool.Get()
				topic := "test"
				msg.TopicPartition.Topic = &topic
				msg.Value = []byte("value")
				pool.Put(msg)
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}
}

// TestMessagePoolMemoryLeak tests that references are properly cleared
func TestMessagePoolMemoryLeak(t *testing.T) {
	pool := NewMessagePool()

	msg := pool.Get()

	// Set all fields
	topic := "test-topic"
	msg.TopicPartition.Topic = &topic
	msg.TopicPartition.Partition = 1
	msg.TopicPartition.Offset = 100
	msg.Value = []byte("test value")
	msg.Key = []byte("test key")
	msg.Headers = []Header{{Key: "header1", Value: []byte("value1")}}
	msg.Opaque = "some opaque data"
	leaderEpoch := int32(5)
	msg.LeaderEpoch = &leaderEpoch
	msg.TopicPartition.LeaderEpoch = &leaderEpoch

	// Return to pool
	pool.Put(msg)

	// Get again and verify all references are cleared
	msg2 := pool.Get()

	if msg2.TopicPartition.Topic != nil {
		t.Error("Topic reference not cleared")
	}
	if msg2.Value != nil {
		t.Error("Value reference not cleared")
	}
	if msg2.Key != nil {
		t.Error("Key reference not cleared")
	}
	if msg2.Headers != nil {
		t.Error("Headers reference not cleared")
	}
	if msg2.Opaque != nil {
		t.Error("Opaque reference not cleared")
	}
	if msg2.LeaderEpoch != nil {
		t.Error("LeaderEpoch reference not cleared")
	}
	if msg2.TopicPartition.LeaderEpoch != nil {
		t.Error("TopicPartition.LeaderEpoch reference not cleared")
	}
	if msg2.TopicPartition.Error != nil {
		t.Error("TopicPartition.Error reference not cleared")
	}
	if msg2.TopicPartition.Partition != 0 {
		t.Error("Partition not reset to 0")
	}
	if msg2.TopicPartition.Offset != 0 {
		t.Error("Offset not reset to 0")
	}
}

// BenchmarkMessageAllocWithoutPool benchmarks message allocation without pooling
func BenchmarkMessageAllocWithoutPool(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		msg := &Message{}
		topic := "test-topic"
		msg.TopicPartition.Topic = &topic
		msg.Value = make([]byte, 100)
		msg.Key = make([]byte, 10)
		// Simulate usage
		_ = msg.Value
	}
}

// BenchmarkMessageAllocWithPool benchmarks message allocation with pooling
func BenchmarkMessageAllocWithPool(b *testing.B) {
	pool := NewMessagePool()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := pool.Get()
		topic := "test-topic"
		msg.TopicPartition.Topic = &topic
		msg.Value = make([]byte, 100)
		msg.Key = make([]byte, 10)
		// Simulate usage
		_ = msg.Value
		pool.Put(msg)
	}
}

// BenchmarkMessagePoolGetPut benchmarks pool Get/Put operations
func BenchmarkMessagePoolGetPut(b *testing.B) {
	pool := NewMessagePool()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := pool.Get()
		pool.Put(msg)
	}
}

// BenchmarkMessagePoolParallel benchmarks concurrent pool access
func BenchmarkMessagePoolParallel(b *testing.B) {
	pool := NewMessagePool()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			msg := pool.Get()
			topic := "test"
			msg.TopicPartition.Topic = &topic
			msg.Value = make([]byte, 100)
			pool.Put(msg)
		}
	})
}
