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

// High-throughput producer example with message pooling enabled
package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {

	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "Usage: %s <broker> <topic>\n",
			os.Args[0])
		os.Exit(1)
	}

	broker := os.Args[1]
	topic := os.Args[2]

	// Create Producer instance with message pooling enabled
	// This reduces GC pressure in high-throughput scenarios (>100K msg/sec)
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":       broker,
		"go.delivery.reports":     false, // Disable delivery reports for max throughput
		"go.message.pool.enable":  true,  // Enable message pooling
		"compression.type":        "lz4",
		"batch.size":              16384,
		"linger.ms":               10,
		"acks":                    1,
	})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Producer %v with message pooling enabled\n", p)

	// Set up signal handling for graceful shutdown
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Produce messages in a loop
	msgCount := 0
	startTime := time.Now()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	fmt.Println("Producing messages... Press Ctrl+C to stop")

	go func() {
		for {
			select {
			case <-sigchan:
				return
			default:
				// Create message
				value := fmt.Sprintf("Message-%d", msgCount)
				err := p.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
					Value:          []byte(value),
				}, nil)

				if err != nil {
					if err.(kafka.Error).Code() == kafka.ErrQueueFull {
						// Producer queue is full, wait a bit
						time.Sleep(10 * time.Millisecond)
						continue
					}
					fmt.Printf("Failed to produce message: %v\n", err)
				} else {
					msgCount++
				}
			}
		}
	}()

	// Print statistics every second
	lastCount := 0
	for {
		select {
		case <-ticker.C:
			elapsed := time.Since(startTime).Seconds()
			rate := float64(msgCount-lastCount)
			totalRate := float64(msgCount) / elapsed
			fmt.Printf("Produced %d messages (current rate: %.0f msg/sec, average: %.0f msg/sec, queue: %d)\n",
				msgCount, rate, totalRate, p.Len())
			lastCount = msgCount

		case <-sigchan:
			fmt.Println("\nShutting down...")
			goto shutdown
		}
	}

shutdown:
	// Flush all outstanding messages
	fmt.Printf("Flushing %d outstanding messages...\n", p.Len())
	unflushed := p.Flush(30000)
	if unflushed > 0 {
		fmt.Printf("Warning: %d messages were not flushed\n", unflushed)
	}

	p.Close()
	elapsed := time.Since(startTime).Seconds()
	fmt.Printf("Produced %d messages in %.2f seconds (%.0f msg/sec average)\n",
		msgCount, elapsed, float64(msgCount)/elapsed)
}
