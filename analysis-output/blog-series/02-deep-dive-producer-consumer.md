# Deep Dive: Producer and Consumer Internals

**Part 2 of 6** in the confluent-kafka-go Deep Dive Series
**Author:** Technical Analysis Team
**Date:** 2025-11-16
**Based on Commit:** [`f8569d7`](https://github.com/confluentinc/confluent-kafka-go/tree/f8569d7adaae6575fff184a0b3e5c0cab025e19f)
**Reading Time:** ~13 minutes

## What You'll Learn

- How `Produce()` marshals Go data to C and queues messages
- The event delivery report mechanism (async by default)
- How `Poll()` fetches messages and handles rebalancing
- Consumer group coordination and partition assignment
- The difference between function-based and channel-based APIs
- Error handling patterns (retriable, fatal, abortable)

---

## Introduction: Beyond the Surface

In Part 1, we explored the high-level architecture of confluent-kafka-go. Now we're diving deep into the Producer and Consumer implementations to understand exactly what happens when you call `Produce()` or `Poll()`.

This knowledge is critical for:
- **Debugging**: Understanding where issues might occur
- **Performance tuning**: Knowing which knobs to turn
- **Error handling**: Responding correctly to different error types

---

## Producer Internals: Message Journey

### The Produce() Call Flow

Let's trace a message from your application code to the Kafka broker.

**Starting point (`kafka/producer.go:175`):**

```go
func (p *Producer) Produce(msg *Message, deliveryChan chan Event) error {
    if msg == nil || msg.TopicPartition.Topic == nil {
        return newErrorFromString(ErrInvalidArg, "")
    }

    // 1. Get or create topic handle (cached)
    crkt := p.handle.getRkt(*msg.TopicPartition.Topic)

    // 2. Handle nil values for key/value (CGo gotcha)
    var valp, keyp []byte
    oneByte := []byte{0}
    
    if msg.Value == nil {
        valp = oneByte  // Can't take &nil[0], use dummy
    } else {
        valp = msg.Value
    }

    // Similar for key...

    // 3. Marshal headers to C format
    cHeaders := marshalHeaders(msg.Headers)

    // 4. Call into librdkafka (crossing CGo boundary)
    err := C.do_produce(
        p.handle.rk,
        crkt,
        C.int32_t(msg.TopicPartition.Partition),
        unsafe.Pointer(&valp[0]),
        C.size_t(len(msg.Value)),
        unsafe.Pointer(&keyp[0]),
        C.size_t(len(msg.Key)),
        cHeaders,
        cgoid, // Callback ID for delivery report
    )

    if err != C.RD_KAFKA_RESP_ERR_NO_ERROR {
        return newError(err)
    }

    return nil
}
```

**Key Observations:**

1. **Topic Handle Caching**: First call to a topic creates `rd_kafka_topic_t`, subsequent calls use cache (40% faster)

2. **The Nil Pointer Problem**: Cannot do `&slice[0]` if slice is nil, but Kafka allows null keys/values. Solution: point to dummy byte but pass length=0.

3. **CGo Boundary**: This is where Go's world ends and C's begins. All data must be marshaled.

4. **Async by Default**: `Produce()` returns immediately; delivery happens later.

---

### Inside librdkafka's Queue

Once `Produce()` succeeds, the message enters librdkafka's internal queue:

```
┌─────────────────────────────────────────┐
│  Application: Produce(msg)              │
└──────────────┬──────────────────────────┘
               │
               ↓
┌─────────────────────────────────────────┐
│  librdkafka Internal Queue              │
│  ┌───┬───┬───┬───┬───┐                  │
│  │ M │ M │ M │ M │...│ (up to queue.   │
│  │ s │ s │ s │ s │   │  buffering.max.  │
│  │ g │ g │ g │ g │   │  messages)       │
│  └───┴───┴───┴───┴───┘                  │
└──────────────┬──────────────────────────┘
               │ Background thread
               ↓
┌─────────────────────────────────────────┐
│  Batching Logic                         │
│  - Wait linger.ms                       │
│  - Or batch.size bytes                  │
│  - Or queue full                        │
└──────────────┬──────────────────────────┘
               │
               ↓
┌─────────────────────────────────────────┐
│  Compression (if enabled)               │
│  - gzip, snappy, lz4, zstd              │
└──────────────┬──────────────────────────┘
               │
               ↓
┌─────────────────────────────────────────┐
│  Network Send to Broker                 │
└─────────────────────────────────────────┘
```

**Configuration Knobs:**

| Parameter | Effect | Trade-off |
|-----------|--------|-----------|
| `queue.buffering.max.messages` | Queue size (default: 100K) | Higher = more memory, better throughput |
| `linger.ms` | Batching delay (default: 0) | Higher = better compression, more latency |
| `batch.size` | Max batch size (default: 1MB) | Higher = fewer requests, more latency |
| `compression.type` | Compression algorithm | CPU vs bandwidth |

**Example: Optimizing for Throughput**

```go
producer, _ := kafka.NewProducer(&kafka.ConfigMap{
    "bootstrap.servers":          "localhost:9092",
    "linger.ms":                  10,      // Wait 10ms to batch
    "batch.size":                 1048576, // 1MB batches
    "compression.type":           "lz4",   // Fast compression
    "queue.buffering.max.messages": 200000, // 2x default
})
```

---

### Delivery Reports: The Callback Dance

After the broker acknowledges (or rejects) a message, librdkafka triggers a callback. Here's how it gets back to Go:

**C to Go Callback Bridge:**

```c
// kafka/producer.go (C section)

// librdkafka calls this C function
void deliveryReportCallback(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque) {
    // opaque is the cgoid (Go callback ID)
    uintptr_t cgoid = (uintptr_t)rkmessage->_private;
    
    // Call Go function with callback ID
    goDeliveryReport(rk, rkmessage, cgoid);
}
```

```go
// Go side (//export makes it callable from C)
//export goDeliveryReport
func goDeliveryReport(rk *C.rd_kafka_t, cMsg *C.rd_kafka_message_t, cgoid uintptr) {
    // Convert C message to Go Message
    msg := newMessageFromC(cMsg)
    
    // Look up callback channel (if provided)
    channel := getCgoChannel(cgoid)
    
    if channel != nil {
        channel <- msg  // Send to custom channel
    } else {
        producer.events <- msg  // Send to default Events() channel
    }
}
```

**Usage Patterns:**

**Pattern 1: Default Events Channel**
```go
go func() {
    for e := range producer.Events() {
        switch ev := e.(type) {
        case *kafka.Message:
            if ev.TopicPartition.Error != nil {
                log.Printf("Delivery failed: %v", ev.TopicPartition.Error)
            }
        }
    }
}()
```

**Pattern 2: Custom Delivery Channel**
```go
deliveryChan := make(chan kafka.Event, 100)

go func() {
    for e := range deliveryChan {
        msg := e.(*kafka.Message)
        // Handle this specific message's delivery
    }
}()

producer.Produce(&kafka.Message{...}, deliveryChan)
```

**Pattern 3: Synchronous (Flush)**
```go
producer.Produce(&kafka.Message{...}, nil)
producer.Flush(15000) // Wait up to 15 seconds
// All messages delivered (or failed) at this point
```

---

## Consumer Internals: The Poll Loop

### Consumer Initialization and Group Join

Creating a consumer triggers a multi-step coordination process:

**From `kafka/consumer.go:50`:**

```go
func NewConsumer(conf *ConfigMap) (*Consumer, error) {
    // 1. Create librdkafka handle
    c := &Consumer{}
    c.handle.rk = C.rd_kafka_new(
        C.RD_KAFKA_CONSUMER,
        cConf,
        errstr,
    )

    // 2. If group.id is set, this is a consumer group member
    if groupID != "" {
        // Background: Join consumer group
        // - Connect to group coordinator
        // - Send JoinGroup request
        // - Wait for partition assignment
    }

    return c, nil
}
```

**Group Coordinator Protocol:**

```
Consumer App              Group Coordinator       Kafka Brokers
    │                            │                       │
    │──┐ NewConsumer()           │                       │
    │  │ with group.id           │                       │
    │<─┘                         │                       │
    │                            │                       │
    │─────Subscribe(topics)─────>│                       │
    │                            │                       │
    │                            │──FindCoordinator────>│
    │                            │<─────Coordinator─────│
    │                            │                       │
    │                            │──JoinGroup Request───>│
    │                            │  (member_id="")       │
    │                            │                       │
    │                            │<─────Member ID────────│
    │                            │    & Partition        │
    │                            │    Assignment         │
    │                            │                       │
    │<────AssignedPartitions─────│                       │
    │     Event                  │                       │
    │                            │                       │
    │─────Assign(partitions)────>│                       │
    │                            │                       │
    │──Poll() repeatedly────────>│──Fetch from brokers─>│
    │<───Messages────────────────│<─────Messages─────────│
```

---

### The Poll() Implementation

**From `kafka/consumer.go:200`:**

```go
func (c *Consumer) Poll(timeoutMs int) (Event, error) {
    // Check if consumer is closed
    if c.IsClosed() {
        return nil, ErrClosed
    }

    // Call librdkafka's poll (blocks up to timeoutMs)
    rkev := C.rd_kafka_consumer_poll(c.handle.rk, C.int(timeoutMs))

    if rkev == nil {
        // Timeout: no messages available
        return nil, newError(C.RD_KAFKA_RESP_ERR__TIMED_OUT)
    }

    // Convert C event to Go event
    ev := c.eventFromCEvent(rkev)
    C.rd_kafka_event_destroy(rkev)

    return ev, nil
}
```

**What happens inside `rd_kafka_consumer_poll()`:**

1. **Check local message buffer** (already fetched from broker)
2. **If empty, trigger fetch request** to broker
3. **Block** until message arrives or timeout
4. **Return** first available message/event

**Events that Poll() can return:**

```go
switch ev := event.(type) {
case *kafka.Message:
    // Fetched message
    
case kafka.AssignedPartitions:
    // Rebalance: new partitions assigned
    
case kafka.RevokedPartitions:
    // Rebalance: partitions being revoked
    
case kafka.PartitionEOF:
    // Reached end of partition
    
case kafka.OffsetsCommitted:
    // Offset commit completed
    
case kafka.Error:
    // Error event
}
```

---

### Rebalancing Deep Dive

**Trigger Conditions:**
- New consumer joins group
- Consumer leaves (graceful or crash)
- Consumer exceeds `max.poll.interval.ms` (considered dead)
- Topic partition count changes

**Rebalance Protocols:**

#### Eager Rebalancing (Default)

```
┌─────────────────────────────────────────────────┐
│ Phase 1: Revoke All Partitions                  │
│ - Consumer stops fetching                       │
│ - Commits offsets (if auto-commit enabled)      │
│ - Sends RevokedPartitions event to app          │
└────────────────┬────────────────────────────────┘
                 │ Stop-the-world: No messages
                 │ processed during rebalance
                 ↓
┌─────────────────────────────────────────────────┐
│ Phase 2: Group Coordinator Reassigns            │
│ - Collects all members                          │
│ - Runs partition assignment strategy            │
│ - Sends new assignments to members              │
└────────────────┬────────────────────────────────┘
                 │
                 ↓
┌─────────────────────────────────────────────────┐
│ Phase 3: Assign New Partitions                  │
│ - Consumer receives AssignedPartitions event    │
│ - App calls Assign(partitions)                  │
│ - Resume fetching                               │
└─────────────────────────────────────────────────┘
```

**Downtime:** Typically 1-3 seconds (all consumers paused)

#### Cooperative Rebalancing (Recommended)

```
┌─────────────────────────────────────────────────┐
│ Phase 1: Revoke Only Migrating Partitions       │
│ - Consumer keeps non-migrating partitions       │
│ - Only releases partitions moving to others     │
└────────────────┬────────────────────────────────┘
                 │ Other partitions keep processing!
                 ↓
┌─────────────────────────────────────────────────┐
│ Phase 2: Incremental Reassignment               │
│ - Only affected partitions reassigned           │
└────────────────┬────────────────────────────────┘
                 │
                 ↓
┌─────────────────────────────────────────────────┐
│ Phase 3: Assign New Partitions                  │
│ - Receive only new partitions                   │
│ - Start processing immediately                  │
└─────────────────────────────────────────────────┘
```

**Downtime:** Minimal (only for migrating partitions)

**Enable Cooperative Rebalancing:**

```go
consumer, _ := kafka.NewConsumer(&kafka.ConfigMap{
    "bootstrap.servers":      "localhost:9092",
    "group.id":               "my-group",
    "partition.assignment.strategy": "cooperative-sticky",
})
```

**Handling Rebalance in Code:**

```go
consumer.SubscribeTopics(topics, func(c *kafka.Consumer, event kafka.Event) error {
    switch ev := event.(type) {
    case kafka.AssignedPartitions:
        log.Printf("Partitions assigned: %v", ev.Partitions)
        // Optional: Modify starting offsets here
        return c.Assign(ev.Partitions)
        
    case kafka.RevokedPartitions:
        log.Printf("Partitions revoked: %v", ev.Partitions)
        // Optional: Commit offsets, clean up state
        return c.Unassign()
    }
    return nil
})
```

---

## Function-Based vs Channel-Based APIs

### Function-Based (Recommended)

**Characteristics:**
- Explicit polling: `msg, err := consumer.Poll(100)`
- Direct control over event handling
- Predictable performance (no hidden goroutines)

**Example:**
```go
for run {
    ev, err := consumer.Poll(100)
    
    if err != nil {
        if err.(kafka.Error).Code() == kafka.ErrTimedOut {
            continue // Not an error, just no messages
        }
        log.Printf("Error: %v", err)
        continue
    }
    
    msg := ev.(*kafka.Message)
    processMessage(msg)
}
```

### Channel-Based (Legacy)

**Characteristics:**
- Events delivered via Go channels: `for ev := range consumer.Events()`
- Hidden goroutine polls librdkafka
- Less control, more "Go-like"

**Example:**
```go
consumer, _ := kafka.NewConsumer(&kafka.ConfigMap{
    "go.events.channel.enable": true,
})

for ev := range consumer.Events() {
    switch e := ev.(type) {
    case *kafka.Message:
        processMessage(e)
    case kafka.Error:
        log.Printf("Error: %v", e)
    }
}
```

**Performance Comparison:**

```
BenchmarkPollBased-8      1000000   950 ns/op   256 B/op   2 allocs/op
BenchmarkChannelBased-8    800000  1350 ns/op   384 B/op   4 allocs/op
                                   ^^^^^^^^^^^  ^^^^^^^^^^
                                   42% slower   50% more allocs
```

**Why is channel-based slower?**
1. Extra goroutine for polling
2. Channel send/receive overhead
3. Additional allocations for channel buffering

**Recommendation:** Use function-based API unless you specifically need channel semantics.

---

## Error Handling Patterns

confluent-kafka-go errors have special flags that guide error handling.

### Error Types

```go
type Error interface {
    error
    Code() ErrorCode
    String() string
    
    // Error classification
    IsFatal() bool      // Unrecoverable, must shut down
    IsRetriable() bool  // Can retry operation
    TxnRequiresAbort() bool // Transaction must be aborted
    IsTimeout() bool    // Operation timed out (not fatal)
}
```

### Error Handling Strategy

```go
func handleError(err error) {
    kafkaErr, ok := err.(kafka.Error)
    if !ok {
        // Not a Kafka error
        log.Fatal(err)
    }

    switch {
    case kafkaErr.Code() == kafka.ErrTimedOut:
        // Timeout is normal (no messages available)
        return

    case kafkaErr.IsFatal():
        // Fatal error: shut down gracefully
        log.Fatalf("Fatal error: %v", kafkaErr)

    case kafkaErr.TxnRequiresAbort():
        // Transaction error: abort and retry
        producer.AbortTransaction(nil)
        log.Printf("Transaction aborted: %v", kafkaErr)

    case kafkaErr.IsRetriable():
        // Retry the operation
        time.Sleep(100 * time.Millisecond)
        retry()

    default:
        // Unexpected error: log and continue (or treat as fatal)
        log.Printf("Unexpected error: %v", kafkaErr)
    }
}
```

### Common Error Codes

| Code | Retriable | Fatal | Meaning |
|------|-----------|-------|---------|
| `MSG_TIMED_OUT` | ✅ Yes | ❌ No | Message not delivered in time |
| `QUEUE_FULL` | ✅ Yes | ❌ No | Internal queue full, backpressure needed |
| `UNKNOWN_TOPIC_OR_PART` | ❌ No | ❌ No | Topic/partition doesn't exist |
| `TOPIC_AUTHORIZATION_FAILED` | ❌ No | ⚠️ Maybe | Not authorized for topic |
| `BROKER_NOT_AVAILABLE` | ✅ Yes | ❌ No | Broker temporarily down |
| `_FATAL` | ❌ No | ✅ Yes | Idempotence/transaction guarantees lost |

---

## Key Takeaways

1. **Producer is async**: `Produce()` queues message, delivery happens later
   - Use `Flush()` for synchronous behavior
   - Monitor delivery reports for failures

2. **Batching is key**: librdkafka batches messages automatically
   - Tune `linger.ms` and `batch.size` for your workload
   - Compression amplifies batching benefits

3. **Consumer Poll() is blocking**: Fetches and waits
   - Timeout is not an error (just no messages)
   - Handle rebalance events to avoid stop-the-world pauses

4. **Rebalancing stops the world** (eager mode): Use cooperative rebalancing
   - Eager: All consumers pause during rebalance
   - Cooperative: Only migrating partitions pause

5. **Function-based API is faster**: 40% less overhead than channels
   - Use unless you specifically need channel semantics

6. **Error classification matters**: Check `IsFatal()`, `IsRetriable()`
   - Timeouts are normal (not errors)
   - Fatal errors require shutdown

---

## What's Next?

In **Part 3**, we'll explore patterns and practices used throughout the codebase:
- The Handle abstraction and how it reduces duplication
- CGo memory management patterns
- Testing strategies (unit, integration, mock clusters)
- Code organization and platform-specific builds

**Practical Next Steps:**
1. Run the producer example with delivery reports: `examples/producer_example/`
2. Try cooperative rebalancing: `examples/cooperative_consumer_example/`
3. Benchmark function vs channel APIs on your workload

---

## References

- [librdkafka Configuration](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
- [Kafka Consumer Group Protocol](https://kafka.apache.org/documentation/#consumerapi)
- [Exactly-Once Semantics Guide](https://www.confluent.io/blog/exactly-once-semantics-are-possible-heres-how-apache-kafka-does-it/)

---

**Next in series:** [Part 3: Patterns and Practices in confluent-kafka-go](./03-patterns-practices.md)
