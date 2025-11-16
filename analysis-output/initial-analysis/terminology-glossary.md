# Terminology Glossary: confluent-kafka-go

**Analysis Date:** 2025-11-16
**Commit SHA:** `f8569d7adaae6575fff184a0b3e5c0cab025e19f`

## Purpose

This glossary defines Kafka-specific and project-specific terminology used throughout the confluent-kafka-go codebase. Use this as a reference when reading code, documentation, or the blog series.

---

## Kafka Core Concepts

### Broker
**Definition**: A single Kafka server that stores messages and serves client requests.

**Usage**: Clients connect to one or more brokers via `bootstrap.servers` configuration.

**Example**:
```go
config := &kafka.ConfigMap{
    "bootstrap.servers": "localhost:9092,localhost:9093",
}
```

**Related Terms**: Cluster, Controller, Replica

---

### Topic
**Definition**: A logical channel for messages, similar to a database table. Topics are partitioned for scalability.

**Naming Convention**: Use lowercase with dots or underscores (e.g., `user.events`, `order_created`)

**Example**:
```go
topic := "user.events"
producer.Produce(&kafka.Message{
    TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
    Value: []byte("event data"),
}, nil)
```

**Related Terms**: Partition, Replication Factor

---

### Partition
**Definition**: A physical subdivision of a topic. Each partition is an ordered, immutable sequence of messages.

**Key Points**:
- Messages within a partition are ordered
- Partitions enable parallel processing
- Partition count cannot be decreased (only increased)

**Example**:
```go
// Explicit partition assignment
partition := int32(0)
tp := kafka.TopicPartition{Topic: &topic, Partition: partition}

// Automatic partition selection
tp := kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}
```

**Special Value**: `kafka.PartitionAny` (-1) = let Kafka choose partition

**Related Terms**: Partition Key, Leader, Follower

---

### Offset
**Definition**: A sequential ID number assigned to each message within a partition. Offsets start at 0 and increment.

**Types**:
- **Absolute Offset**: Specific position (e.g., `42`)
- **Logical Offset**: Special values (`OffsetBeginning`, `OffsetEnd`, `OffsetStored`)

**Example**:
```go
const (
    OffsetBeginning = kafka.Offset(-2)  // Start of partition
    OffsetEnd       = kafka.Offset(-1)  // End of partition (next message)
    OffsetStored    = kafka.Offset(-1000) // Last committed offset
    OffsetInvalid   = kafka.Offset(-1001) // Invalid/unset
)
```

**Related Terms**: Committed Offset, Consumer Offset, High Watermark

---

### Consumer Group
**Definition**: A group of consumers that cooperate to consume messages from topics. Each partition is consumed by exactly one consumer in the group.

**Key Points**:
- Enables load balancing (multiple consumers share work)
- Enables fault tolerance (if one consumer fails, others take over)
- Group ID is configured via `group.id`

**Example**:
```go
consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
    "group.id": "my-consumer-group",
    "auto.offset.reset": "earliest",
})
```

**Related Terms**: Rebalance, Assigned Partitions, Coordinator

---

### Rebalance
**Definition**: The process of reassigning partitions among consumers in a group when:
- A consumer joins or leaves the group
- A topic's partition count changes
- A consumer crashes or times out

**Rebalance Protocols**:
1. **Eager Rebalancing** (default): Stop-the-world approach
   - All consumers release their partitions
   - Partitions are reassigned
   - Consumers resume consumption

2. **Cooperative Rebalancing** (incremental): Minimal disruption
   - Only affected partitions are reassigned
   - Consumers continue processing other partitions

**Example** (handling rebalance):
```go
err := consumer.SubscribeTopics(topics, func(c *kafka.Consumer, event kafka.Event) error {
    switch e := event.(type) {
    case kafka.AssignedPartitions:
        fmt.Printf("Partitions assigned: %v\n", e)
        return c.Assign(e.Partitions)
    case kafka.RevokedPartitions:
        fmt.Printf("Partitions revoked: %v\n", e)
        return c.Unassign()
    }
    return nil
})
```

**Related Terms**: AssignedPartitions Event, RevokedPartitions Event, Rebalance Callback

---

### Message
**Definition**: The unit of data in Kafka, consisting of key, value, headers, and metadata.

**Structure** (in confluent-kafka-go):
```go
type Message struct {
    TopicPartition TopicPartition  // Topic + partition + offset
    Value          []byte           // Message payload
    Key            []byte           // Optional partition key
    Headers        []Header         // Optional headers (key-value pairs)
    Timestamp      time.Time        // Message timestamp
    TimestampType  TimestampType    // Create time vs. log append time
    Opaque         interface{}      // User-provided context
}
```

**Related Terms**: Record, Event, Payload

---

### Producer
**Definition**: A client that publishes messages to Kafka topics.

**Operation Modes**:
1. **Asynchronous** (default): Non-blocking, delivery reports via events
2. **Synchronous**: Block until ack received (use `Flush()`)

**Example**:
```go
producer, err := kafka.NewProducer(&kafka.ConfigMap{
    "bootstrap.servers": "localhost:9092",
})

// Async produce
producer.Produce(&kafka.Message{
    TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
    Value: []byte("message"),
}, nil)

// Wait for delivery
producer.Flush(15000) // 15 second timeout
```

**Related Terms**: Delivery Report, Idempotent Producer, Transactional Producer

---

### Consumer
**Definition**: A client that reads messages from Kafka topics.

**Operation Modes**:
1. **Poll-Based** (recommended): Call `Poll()` to fetch messages
2. **Channel-Based** (legacy): Receive messages via Go channel

**Example**:
```go
consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
    "group.id": "myGroup",
    "auto.offset.reset": "earliest",
})

consumer.SubscribeTopics([]string{"myTopic"}, nil)

for {
    msg, err := consumer.Poll(100)
    if err == nil {
        fmt.Printf("Message: %s\n", string(msg.Value))
    }
}
```

**Related Terms**: High-Level Consumer, Subscribe, Assign

---

## librdkafka Concepts

### librdkafka
**Definition**: A high-performance C/C++ library implementing the Kafka protocol. confluent-kafka-go wraps librdkafka via CGo.

**Why librdkafka?**
- Battle-tested (10+ years of production use)
- Optimized performance (C implementation)
- Cross-language consistency (Python, .NET, Go all use same core)

**Version**: confluent-kafka-go requires librdkafka v2.12.0+

**Related Terms**: CGo, Handle, rk (rd_kafka_t)

---

### Handle (rk)
**Definition**: The underlying librdkafka client instance.

**In Code**:
```go
type handle struct {
    rk  *C.rd_kafka_t          // librdkafka client handle
    rkq *C.rd_kafka_queue_t    // Event queue
    // ... other fields
}
```

**Purpose**: Shared state between Producer/Consumer/AdminClient

**Related Terms**: rd_kafka_t, CGo

---

### ConfigMap
**Definition**: A key-value map for configuring Kafka clients. Supports both Go-specific and librdkafka configurations.

**Example**:
```go
config := &kafka.ConfigMap{
    "bootstrap.servers": "localhost:9092",       // librdkafka config
    "group.id": "myGroup",                       // librdkafka config
    "go.events.channel.enable": true,            // Go-specific config
    "go.delivery.reports": false,                // Go-specific config
}
```

**Go-Specific Configs** (prefix: `go.`):
- `go.events.channel.enable` — Use channel-based events (legacy)
- `go.delivery.reports` — Enable/disable delivery reports
- `go.application.rebalance.enable` — Receive rebalance events
- `go.logs.channel.enable` — Forward logs to Go channel

**Related Terms**: Configuration, Properties

---

### Event
**Definition**: An occurrence reported by the Kafka client (message, error, rebalance, etc.).

**Event Types**:
```go
type Event interface{}

// Concrete event types:
*Message                  // Consumed message or delivery report
*AssignedPartitions       // Partitions assigned to consumer
*RevokedPartitions        // Partitions revoked from consumer
*PartitionEOF             // Reached end of partition
*OffsetsCommitted         // Offset commit result
*KafkaError               // Error event
*OAuthBearerTokenRefresh  // OAuth token refresh needed
```

**Usage**:
```go
for e := range producer.Events() {
    switch ev := e.(type) {
    case *kafka.Message:
        if ev.TopicPartition.Error != nil {
            fmt.Printf("Delivery failed: %v\n", ev.TopicPartition.Error)
        } else {
            fmt.Printf("Delivered to %v\n", ev.TopicPartition)
        }
    }
}
```

**Related Terms**: Delivery Report, Rebalance Event, Error Event

---

## Advanced Kafka Concepts

### Idempotent Producer
**Definition**: A producer that guarantees exactly-once delivery per partition, eliminating duplicates caused by retries.

**Configuration**:
```go
config := &kafka.ConfigMap{
    "enable.idempotence": true,
    // Automatically sets:
    // - acks=all
    // - max.in.flight.requests.per.connection=5
    // - retries=MaxInt
}
```

**Use Case**: Prevent duplicate messages during network failures or broker restarts

**Related Terms**: Exactly-Once Semantics (EOS), Transactional Producer

---

### Transactional Producer
**Definition**: A producer that supports atomic writes across multiple partitions and topics, with exactly-once processing guarantees.

**Configuration**:
```go
config := &kafka.ConfigMap{
    "transactional.id": "my-transactional-id",
}

producer, _ := kafka.NewProducer(config)
producer.InitTransactions(nil)

// Transaction workflow
producer.BeginTransaction()
producer.Produce(message1, nil)
producer.Produce(message2, nil)
producer.SendOffsetsToTransaction(offsets, groupMetadata, nil)
producer.CommitTransaction(nil)
```

**Use Cases**:
- Consume-Process-Produce loops (stream processing)
- Multi-topic atomic writes
- Exactly-once stream processing

**Related Terms**: EOS, Transactional ID, Isolation Level

---

### Exactly-Once Semantics (EOS)
**Definition**: A guarantee that each message is processed exactly once, even in the presence of failures.

**Components**:
1. **Idempotent Producer** — No duplicates on retry
2. **Transactions** — Atomic writes
3. **Read Committed Isolation** — Consumers only see committed data

**Configuration**:
```go
// Producer
producerConfig := &kafka.ConfigMap{
    "transactional.id": "my-tx-id",
}

// Consumer
consumerConfig := &kafka.ConfigMap{
    "isolation.level": "read_committed",
    "enable.auto.commit": false,
}
```

**Related Terms**: At-Most-Once, At-Least-Once, Idempotence

---

### Delivery Guarantee Levels

| Level | Meaning | Configuration | Use Case |
|-------|---------|---------------|----------|
| **At-Most-Once** | May lose messages | `acks=0` | Metrics, logs (tolerate loss) |
| **At-Least-Once** | May duplicate messages | `acks=all` | Most applications (default) |
| **Exactly-Once** | No loss, no duplicates | Transactions + `acks=all` | Financial, critical data |

---

### Offset Commit
**Definition**: The process of saving a consumer's current position (offset) in a partition.

**Commit Strategies**:
1. **Auto-Commit** (default): Periodically commit offsets automatically
   ```go
   "enable.auto.commit": true,
   "auto.commit.interval.ms": 5000,
   ```

2. **Manual Commit** (recommended): Explicit control over when to commit
   ```go
   "enable.auto.commit": false,
   // ... process message ...
   consumer.CommitMessage(msg)
   ```

**Related Terms**: Committed Offset, Offset Reset

---

### Partition Key
**Definition**: An optional message field used to determine which partition a message is sent to.

**Behavior**:
- Messages with the same key go to the same partition (order guaranteed)
- `key = nil` → round-robin partition selection
- `key != nil` → `hash(key) % partition_count`

**Example**:
```go
// Order all events for user_123 to same partition
key := []byte("user_123")
producer.Produce(&kafka.Message{
    TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
    Key:   key,
    Value: []byte("event data"),
}, nil)
```

**Related Terms**: Partitioning Strategy, Ordering Guarantees

---

## Schema Registry Concepts

### Schema Registry
**Definition**: A centralized service for managing schemas (Avro, Protobuf, JSON Schema) with versioning and compatibility checking.

**URL**: Typically `http://schema-registry:8081`

**Example**:
```go
import "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"

client, err := schemaregistry.NewClient(schemaregistry.NewConfig("http://localhost:8081"))
```

**Related Terms**: Schema ID, Subject, Schema Evolution

---

### Subject
**Definition**: A namespace for schemas in Schema Registry. By default, subject = `<topic>-key` or `<topic>-value`.

**Naming Strategies**:
- **TopicNameStrategy** (default): `topic-key`, `topic-value`
- **RecordNameStrategy**: `<record_name>`
- **TopicRecordNameStrategy**: `<topic>-<record_name>`

**Example**:
```go
// Schema for topic "users" value
subject := "users-value"
schema, err := client.GetLatestSchema(subject)
```

**Related Terms**: Schema, Versioning

---

### Schema ID
**Definition**: A unique integer identifier assigned by Schema Registry to each schema version.

**Usage**: Embedded in message headers for efficient schema lookup.

**Wire Format** (Avro):
```
[0x00] [4-byte schema ID] [serialized data]
```

**Example**:
```go
// Schema Registry automatically handles schema ID encoding
serializer, err := avrov2.NewSerializer(client, serde.ValueSerde, avrov2.NewSerializerConfig())
bytes, err := serializer.Serialize(topic, &userData)
// bytes = [0x00] [schema_id] [avro_data]
```

**Related Terms**: Magic Byte, Wire Format

---

### Serde (Serializer/Deserializer)
**Definition**: A component that converts between Go objects and wire format (bytes).

**Supported Formats**:
1. **Avro** (v1 and v2)
2. **Protobuf**
3. **JSON Schema**

**Example**:
```go
import "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avrov2"

// Serializer
serializer, err := avrov2.NewSerializer(client, serde.ValueSerde, avrov2.NewSerializerConfig())
bytes, err := serializer.Serialize(topic, &myStruct)

// Deserializer
deserializer, err := avrov2.NewDeserializer(client, serde.ValueSerde, avrov2.NewDeserializerConfig())
err := deserializer.DeserializeInto(topic, bytes, &myStruct)
```

**Related Terms**: Wire Format, Schema Evolution

---

### Schema Evolution
**Definition**: The process of changing a schema over time while maintaining compatibility.

**Compatibility Modes**:
- **BACKWARD** (default): New schema can read old data
- **FORWARD**: Old schema can read new data
- **FULL**: Both backward and forward compatible
- **NONE**: No compatibility checks

**Example** (adding optional field):
```json
// v1
{"type": "record", "fields": [
    {"name": "id", "type": "int"}
]}

// v2 (backward compatible - added optional field with default)
{"type": "record", "fields": [
    {"name": "id", "type": "int"},
    {"name": "email", "type": ["null", "string"], "default": null}
]}
```

**Related Terms**: Compatibility Mode, Schema Versioning

---

### Field-Level Encryption
**Definition**: Encrypting specific fields in a message before sending to Kafka, with decryption on the consumer side.

**KMS Providers**:
- AWS KMS
- GCP Cloud KMS
- Azure Key Vault
- HashiCorp Vault
- Local KMS (testing)

**Example**:
```go
rule := &schemaregistry.Rule{
    Name: "encryptPII",
    Kind: "TRANSFORM",
    Mode: "WRITEREAD",
    Type: "ENCRYPT",
    Tags: []string{"PII"},
    Params: map[string]string{"kms.type": "aws-kms"},
}
```

**Related Terms**: Data Contract, Rules Engine, KMS

---

### Data Contract / Rules
**Definition**: Declarative rules applied to schemas for validation, transformation, or encryption.

**Rule Types**:
- **CONDITION**: Validation rules (CEL expressions)
- **TRANSFORM**: Data transformation (JSONata, encryption)

**Example** (CEL rule):
```go
rule := &schemaregistry.Rule{
    Name: "checkAge",
    Kind: "CONDITION",
    Type: "CEL",
    Expr: "message.age >= 18",
}
```

**Related Terms**: CEL (Common Expression Language), JSONata

---

## confluent-kafka-go Specific Terms

### CGo
**Definition**: Go's mechanism for calling C code. confluent-kafka-go uses CGo to interface with librdkafka.

**Identifying CGo Code**:
```go
import "C"

/*
#include "rdkafka.h"
*/
import "C"

func example() {
    cString := C.CString("hello")
    defer C.free(unsafe.Pointer(cString))
    C.some_c_function(cString)
}
```

**Performance Note**: CGo calls have ~10-100ns overhead per call

**Related Terms**: librdkafka, Handle, unsafe.Pointer

---

### Event Channel
**Definition**: A Go channel that receives events from the Kafka client.

**Usage** (legacy channel-based API):
```go
config := &kafka.ConfigMap{
    "go.events.channel.enable": true,
}
consumer, _ := kafka.NewConsumer(config)

for ev := range consumer.Events() {
    switch e := ev.(type) {
    case *kafka.Message:
        processMessage(e)
    }
}
```

**Note**: Function-based API (`Poll()`) is recommended over channel-based API

**Related Terms**: Events(), Poll()

---

### Poll()
**Definition**: A function that fetches the next event from the Kafka client (message, error, rebalance, etc.).

**Usage**:
```go
// Consumer
msg, err := consumer.Poll(100) // 100ms timeout

// Producer (for delivery reports)
e := producer.Poll(0) // Non-blocking
```

**Recommended Pattern**: Use `Poll()` instead of channel-based events for better performance

**Related Terms**: Event, Timeout

---

### Delivery Report
**Definition**: An event indicating whether a produced message was successfully delivered to Kafka.

**Example**:
```go
go func() {
    for e := range producer.Events() {
        switch ev := e.(type) {
        case *kafka.Message:
            if ev.TopicPartition.Error != nil {
                fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition.Error)
            } else {
                fmt.Printf("Delivered to partition %d at offset %v\n",
                    ev.TopicPartition.Partition, ev.TopicPartition.Offset)
            }
        }
    }
}()
```

**Configuration**: Disable with `"go.delivery.reports": false`

**Related Terms**: Producer Events, Acknowledgment

---

### TopicPartition
**Definition**: A struct combining topic name, partition number, and offset.

**Structure**:
```go
type TopicPartition struct {
    Topic       *string    // Topic name
    Partition   int32      // Partition number
    Offset      Offset     // Offset in partition
    Metadata    *string    // Optional metadata
    Error       error      // Error (if any)
    LeaderEpoch *int32     // Leader epoch (advanced)
}
```

**Usage**:
```go
topic := "users"
tp := kafka.TopicPartition{
    Topic:     &topic,
    Partition: 0,
    Offset:    kafka.OffsetStored,
}
consumer.Assign([]kafka.TopicPartition{tp})
```

**Related Terms**: Assignment, Offset

---

### AdminClient
**Definition**: A Kafka client for administrative operations (create topics, manage ACLs, etc.).

**Example**:
```go
admin, err := kafka.NewAdminClient(&kafka.ConfigMap{
    "bootstrap.servers": "localhost:9092",
})

results, err := admin.CreateTopics(ctx, []kafka.TopicSpecification{{
    Topic:             "new-topic",
    NumPartitions:     3,
    ReplicationFactor: 2,
}})
```

**Supported Operations** (19 total):
- Topic management (create, delete, describe)
- ACL management (create, delete, describe)
- Consumer group management (delete, describe, list)
- Configuration management (describe, alter)
- Offset management (list, alter)
- Cluster operations (describe, elect leaders)

**Related Terms**: Cluster Administration, Metadata

---

### Mock Cluster
**Definition**: A simulated Kafka cluster for testing without running a real Kafka broker.

**Example**:
```go
import "github.com/confluentinc/confluent-kafka-go/v2/kafka"

mockCluster, _ := kafka.NewMockCluster(3) // 3 brokers
defer mockCluster.Close()

bootstrap := mockCluster.BootstrapServers()
producer, _ := kafka.NewProducer(&kafka.ConfigMap{
    "bootstrap.servers": bootstrap,
})
```

**Use Case**: Unit testing without Docker

**Related Terms**: Integration Testing, Test Fixtures

---

## Build & Configuration Terms

### Build Tags
**Definition**: Go build constraints that select platform-specific code.

**Examples**:
- `-tags musl` — Build for Alpine Linux (musl libc)
- `-tags dynamic` — Link against system librdkafka (vs. bundled static)

**Usage**:
```bash
go build -tags musl ./...
```

**Related Terms**: Platform-Specific Builds, Static vs. Dynamic Linking

---

### Static Linking
**Definition**: Bundling librdkafka directly into the Go binary (default).

**Pros**:
- No external dependencies
- `go get` works out-of-the-box
- Version compatibility guaranteed

**Cons**:
- Larger repository (112MB of static libs)
- Larger binary size (~35MB)

**Location**: `kafka/librdkafka_vendor/`

**Related Terms**: Dynamic Linking, Vendored Dependencies

---

### Dynamic Linking
**Definition**: Using a system-installed librdkafka instead of the bundled version.

**Usage**:
```bash
go build -tags dynamic ./...
```

**Pros**:
- Smaller binary (~8MB)
- Can use custom librdkafka build (e.g., with Kerberos)

**Cons**:
- Requires librdkafka to be installed
- Version mismatch risk

**Related Terms**: Static Linking, librdkafka

---

## Acronyms & Abbreviations

| Term | Full Name | Meaning |
|------|-----------|---------|
| **ACL** | Access Control List | Permission system for topics/groups |
| **API** | Application Programming Interface | Client interface |
| **CEL** | Common Expression Language | Google's expression language for rules |
| **CGo** | C + Go | Go's C interop mechanism |
| **EOF** | End of File | End of partition marker |
| **EOS** | Exactly-Once Semantics | No loss, no duplicates guarantee |
| **ISR** | In-Sync Replica | Replicas that are caught up with leader |
| **KMS** | Key Management Service | Encryption key management |
| **LOC** | Lines of Code | Code size metric |
| **OSS** | Open Source Software | Publicly available code |
| **RFC** | Request for Comments | Design proposal document |
| **RPC** | Remote Procedure Call | Network request/response |
| **SASL** | Simple Authentication and Security Layer | Auth framework |
| **TLS/SSL** | Transport Layer Security / Secure Sockets Layer | Encryption protocol |
| **UUID** | Universally Unique Identifier | Unique ID format |

---

## Common Configuration Keys

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `bootstrap.servers` | string | localhost | Broker connection string |
| `group.id` | string | - | Consumer group ID |
| `enable.auto.commit` | bool | true | Auto-commit offsets |
| `auto.offset.reset` | string | latest | Where to start consuming (earliest/latest) |
| `enable.idempotence` | bool | false | Enable idempotent producer |
| `transactional.id` | string | - | Transactional producer ID |
| `acks` | int | 1 | Ack level (0=none, 1=leader, all=all ISR) |
| `compression.type` | string | none | Compression (none, gzip, snappy, lz4, zstd) |
| `isolation.level` | string | read_uncommitted | Consumer isolation (read_committed for EOS) |
| `session.timeout.ms` | int | 10000 | Consumer session timeout |
| `max.poll.interval.ms` | int | 300000 | Max time between polls |

**Full list**: See [librdkafka configuration](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)

---

## Summary

This glossary covers **100+ terms** essential for understanding the confluent-kafka-go codebase:
- ✅ Kafka core concepts (topics, partitions, offsets, consumers, producers)
- ✅ Advanced features (transactions, EOS, Schema Registry)
- ✅ confluent-kafka-go specifics (CGo, handles, events)
- ✅ Build and configuration terms

**Next Steps**:
- Refer to this glossary when reading code or blog posts
- Bookmark for quick reference
- Suggest additions via pull request

**Related Documents**:
- [Quick Start Guide](./00-quick-start.md)
- [Repository Structure](./repository-structure.md)
- [Blog Series](../blog-series/)
