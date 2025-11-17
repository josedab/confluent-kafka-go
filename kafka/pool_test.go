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
	"testing"
	"time"
)

func TestNewMessagePool(t *testing.T) {
	pool := NewMessagePool()
	if pool == nil {
		t.Fatal("NewMessagePool returned nil")
	}
}

func TestMessagePool_GetPut(t *testing.T) {
	pool := NewMessagePool()

	// Get message from pool
	msg := pool.Get()
	if msg == nil {
		t.Fatal("Get() returned nil")
	}

	// Populate message
	topic := "test-topic"
	msg.TopicPartition = TopicPartition{Topic: &topic, Partition: 0}
	msg.Value = []byte("test value")
	msg.Key = []byte("test key")
	msg.Headers = []Header{{Key: "header1", Value: []byte("value1")}}
	msg.Timestamp = time.Now()
	msg.TimestampType = TimestampCreateTime

	// Return to pool
	pool.Put(msg)

	// Verify message was cleared
	if msg.Value != nil {
		t.Error("Value not cleared after Put()")
	}
	if msg.Key != nil {
		t.Error("Key not cleared after Put()")
	}
	if msg.Headers != nil {
		t.Error("Headers not cleared after Put()")
	}
	if msg.TopicPartition.Topic != nil {
		t.Error("TopicPartition.Topic not cleared after Put()")
	}
}

func TestMessagePool_Reuse(t *testing.T) {
	pool := NewMessagePool()

	// Get and return a message
	msg1 := pool.Get()
	pool.Put(msg1)

	// Get another message - should reuse the first one
	msg2 := pool.Get()

	// They should be the same underlying object
	// Note: This is not guaranteed by sync.Pool but is likely
	if msg1 != msg2 {
		t.Log("Messages are different (this is OK, sync.Pool doesn't guarantee reuse)")
	}
}

func TestMessagePool_PutNil(t *testing.T) {
	pool := NewMessagePool()

	// Should not panic
	pool.Put(nil)
}

func TestMessagePool_ConcurrentAccess(t *testing.T) {
	pool := NewMessagePool()
	done := make(chan bool)

	// Multiple goroutines accessing pool concurrently
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				msg := pool.Get()
				msg.Value = []byte("test")
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

func TestGlobalMessagePool(t *testing.T) {
	// Save original state
	original := GetGlobalMessagePool()
	defer SetGlobalMessagePool(original)

	// Initially should be nil (or whatever was set before)
	SetGlobalMessagePool(nil)
	if GetGlobalMessagePool() != nil {
		t.Error("Global pool should be nil after SetGlobalMessagePool(nil)")
	}

	// Set global pool
	pool := NewMessagePool()
	SetGlobalMessagePool(pool)

	if GetGlobalMessagePool() != pool {
		t.Error("GetGlobalMessagePool didn't return the set pool")
	}
}

func TestEnableDisableGlobalPooling(t *testing.T) {
	// Save original state
	original := GetGlobalMessagePool()
	defer SetGlobalMessagePool(original)

	// Enable
	EnableGlobalPooling()
	if GetGlobalMessagePool() == nil {
		t.Error("Global pool should not be nil after EnableGlobalPooling()")
	}

	// Disable
	DisableGlobalPooling()
	if GetGlobalMessagePool() != nil {
		t.Error("Global pool should be nil after DisableGlobalPooling()")
	}
}

func TestPoolStats(t *testing.T) {
	pool := NewMessagePool()
	stats := pool.GetStats()

	if !stats.Enabled {
		t.Error("Pool stats should show Enabled=true")
	}
}

func TestMessageClearing(t *testing.T) {
	pool := NewMessagePool()
	msg := pool.Get()

	// Set all fields
	topic := "test"
	epoch := int32(5)
	msg.TopicPartition = TopicPartition{
		Topic:     &topic,
		Partition: 3,
		Offset:    100,
		Metadata:  &topic,
		Error:     NewError(ErrUnknown, "test", false),
	}
	msg.Value = []byte("value")
	msg.Key = []byte("key")
	msg.Headers = []Header{{Key: "k", Value: []byte("v")}}
	msg.Timestamp = time.Now()
	msg.TimestampType = TimestampCreateTime
	msg.Opaque = "opaque"
	msg.LeaderEpoch = &epoch

	// Put back in pool (clears all fields)
	pool.Put(msg)

	// Verify all fields are cleared
	if msg.Value != nil {
		t.Error("Value not cleared")
	}
	if msg.Key != nil {
		t.Error("Key not cleared")
	}
	if msg.Headers != nil {
		t.Error("Headers not cleared")
	}
	if msg.TopicPartition.Topic != nil {
		t.Error("Topic not cleared")
	}
	if msg.TopicPartition.Partition != PartitionAny {
		t.Error("Partition not reset")
	}
	if msg.TopicPartition.Offset != OffsetInvalid {
		t.Error("Offset not reset")
	}
	if msg.TopicPartition.Metadata != nil {
		t.Error("Metadata not cleared")
	}
	if msg.TopicPartition.Error != nil {
		t.Error("Error not cleared")
	}
	if msg.TimestampType != TimestampNotAvailable {
		t.Error("TimestampType not reset")
	}
	if msg.Opaque != nil {
		t.Error("Opaque not cleared")
	}
	if msg.LeaderEpoch != nil {
		t.Error("LeaderEpoch not cleared")
	}
}

// Benchmark: Message allocation without pooling
func BenchmarkMessageAllocNoPool(b *testing.B) {
	topic := "benchmark-topic"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := &Message{
			TopicPartition: TopicPartition{Topic: &topic, Partition: 0},
			Value:          []byte("benchmark data"),
		}
		_ = msg
	}
}

// Benchmark: Message allocation with pooling
func BenchmarkMessageAllocWithPool(b *testing.B) {
	pool := NewMessagePool()
	topic := "benchmark-topic"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := pool.Get()
		msg.TopicPartition = TopicPartition{Topic: &topic, Partition: 0}
		msg.Value = []byte("benchmark data")
		pool.Put(msg)
	}
}

// Benchmark: Concurrent pool access
func BenchmarkMessagePoolConcurrent(b *testing.B) {
	pool := NewMessagePool()
	topic := "benchmark-topic"

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			msg := pool.Get()
			msg.TopicPartition = TopicPartition{Topic: &topic, Partition: 0}
			msg.Value = []byte("data")
			pool.Put(msg)
		}
	})
}

// Benchmark: Pool Get operation
func BenchmarkPoolGet(b *testing.B) {
	pool := NewMessagePool()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := pool.Get()
		_ = msg
	}
}

// Benchmark: Pool Put operation
func BenchmarkPoolPut(b *testing.B) {
	pool := NewMessagePool()
	messages := make([]*Message, b.N)
	for i := 0; i < b.N; i++ {
		messages[i] = pool.Get()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.Put(messages[i])
	}
}
