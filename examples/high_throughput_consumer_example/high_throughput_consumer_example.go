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

// High-throughput consumer example with message pooling enabled
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

	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "Usage: %s <broker> <group> <topics..>\n",
			os.Args[0])
		os.Exit(1)
	}

	broker := os.Args[1]
	group := os.Args[2]
	topics := os.Args[3:]

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Create Consumer instance with message pooling enabled
	// This reduces GC pressure in high-throughput scenarios (>100K msg/sec)
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":       broker,
		"group.id":                group,
		"auto.offset.reset":       "earliest",
		"go.message.pool.enable":  true,  // Enable message pooling
		"fetch.min.bytes":         1,
		"fetch.wait.max.ms":       100,
		"max.partition.fetch.bytes": 1048576,
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v with message pooling enabled\n", c)

	err = c.SubscribeTopics(topics, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to subscribe to topics: %s\n", err)
		os.Exit(1)
	}

	msgCount := 0
	startTime := time.Now()
	lastPrintTime := startTime
	lastCount := 0

	fmt.Println("Consuming messages... Press Ctrl+C to stop")

	run := true
	for run {
		select {
		case <-sigchan:
			fmt.Println("\nShutting down...")
			run = false

		default:
			ev := c.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				msgCount++

				// Process the message here
				// Note: With pooling enabled, messages should not be retained
				// beyond the scope of processing. If you need to keep the message,
				// make a copy of the data you need.

				// Example: just count the message
				_ = e.Value

				// Print statistics every second
				now := time.Now()
				if now.Sub(lastPrintTime) >= time.Second {
					elapsed := now.Sub(startTime).Seconds()
					rate := float64(msgCount - lastCount)
					totalRate := float64(msgCount) / elapsed
					fmt.Printf("Consumed %d messages (current rate: %.0f msg/sec, average: %.0f msg/sec)\n",
						msgCount, rate, totalRate)
					lastCount = msgCount
					lastPrintTime = now
				}

			case kafka.Error:
				// Errors are typically just informational, the client will try to
				// automatically recover
				fmt.Fprintf(os.Stderr, "Error: %v\n", e)

			default:
				// Ignore other event types
			}
		}
	}

	c.Close()

	elapsed := time.Since(startTime).Seconds()
	fmt.Printf("Consumed %d messages in %.2f seconds (%.0f msg/sec average)\n",
		msgCount, elapsed, float64(msgCount)/elapsed)
}
