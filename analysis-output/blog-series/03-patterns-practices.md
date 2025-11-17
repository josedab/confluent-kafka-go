# Patterns and Practices in confluent-kafka-go

**Part 3 of 6** in the confluent-kafka-go Deep Dive Series
**Author:** Technical Analysis Team
**Date:** 2025-11-16
**Reading Time:** ~11 minutes

## What You'll Learn

- The Handle abstraction pattern and why it exists
- CGo memory management patterns (CString, defer free, nil handling)
- Error handling strategies and code generation
- Testing patterns (unit, integration, mock clusters)
- Platform-specific code organization with build tags

---

## The Handle Pattern: Shared Infrastructure

### Problem: Code Duplication

Producer, Consumer, and AdminClient all need:
- librdkafka client handle (`rd_kafka_t`)
- Topic metadata caching
- Event queue management
- Logging infrastructure  
- CGo callback registration

Without abstraction: 3x duplication across client types.

### Solution: Common Handle

**From `kafka/handle.go:88`:**

```go
type handle struct {
    rk  *C.rd_kafka_t       // librdkafka handle
    rkq *C.rd_kafka_queue_t // Event queue

    // Topic caches (avoid CGo overhead)
    rktCacheLock sync.Mutex
    rktCache     map[string]*C.rd_kafka_topic_t
    rktNameCache map[*C.rd_kafka_topic_t]string

    // CGo callback mapping
    cgoLock   sync.Mutex
    cgoidNext uintptr
    cgomap    map[int]cgoif

    // Client-specific back-references
    p *Producer  // nil for Consumer/Admin
    c *Consumer  // nil for Producer/Admin

    // Cached instance name
    name string

    waitGroup sync.WaitGroup
}
```

**Benefits:**
- ✅ Single lifecycle implementation
- ✅ Shared caching (40% faster topic lookups)
- ✅ Centralized logging
- ✅ Reduced test surface area

---

## CGo Memory Management Patterns

### Pattern 1: CString + Defer Free

**The Problem:**
```go
// WRONG: Memory leak!
topic := C.CString("my-topic")
C.rd_kafka_topic_new(rk, topic, nil)
// topic memory never freed
```

**The Pattern:**
```go
// CORRECT: Always defer free
topic := C.CString("my-topic")
defer C.free(unsafe.Pointer(topic))
C.rd_kafka_topic_new(rk, topic, nil)
```

**Why defer?** If `rd_kafka_topic_new()` panics, free still executes.

### Pattern 2: Nil Slice Handling

**The Problem:**
```go
var value []byte // nil slice
ptr := unsafe.Pointer(&value[0]) // PANIC!
```

**The Pattern:**
```go
var valp []byte
oneByte := []byte{0}

if msg.Value == nil {
    valp = oneByte // Point to dummy, pass len=0
} else {
    valp = msg.Value
}

C.do_produce(..., unsafe.Pointer(&valp[0]), C.size_t(len(msg.Value)))
//                                            ^^^^^^^^^^^^^^^^^^^^^^^
//                                            Actual length (could be 0)
```

### Pattern 3: C Callback to Go

**Problem:** C can't call Go functions directly (no GC, different stack)

**Solution:** Export Go function, use callback IDs

```go
// C code can call this
//export goDeliveryReport
func goDeliveryReport(rk *C.rd_kafka_t, msg *C.rd_kafka_message_t, cgoid uintptr) {
    // Look up Go callback by ID
    callback := getCgoCallback(cgoid)
    callback.deliverMessage(newMessageFromC(msg))
}
```

---

## Error Handling Strategies

### Generated Error Codes

confluent-kafka-go auto-generates error codes from librdkafka:

**Tool: `kafka/error_gen.go`**

```go
// Reads librdkafka headers, generates:
const (
    ErrBrokerNotAvailable     = ErrorCode(-10)
    ErrUnknownTopicOrPartition = ErrorCode(3)
    // ... 100+ error codes
)
```

**Why generate?** Ensures sync with librdkafka versions.

### Error Wrapping

```go
func newError(cErr C.rd_kafka_resp_err_t) Error {
    return Error{
        code: ErrorCode(cErr),
        str:  C.GoString(C.rd_kafka_err2str(cErr)),
    }
}
```

### Context-Aware Errors (Future: RFC-0009)

```go
// Enhanced error with actionable context
type EnhancedError struct {
    Code       ErrorCode
    Message    string
    Causes     []string  // Possible causes
    Solutions  []string  // How to fix
    ConfigHint *ConfigHint
}
```

---

## Testing Patterns

### Unit Tests

**Approach:** Test individual functions with mock data

**Example: `kafka/config_test.go`:**

```go
func TestConfigSet(t *testing.T) {
    conf := &ConfigMap{}
    err := conf.SetKey("bootstrap.servers", "localhost:9092")
    assert.NoError(t, err)
    
    val, err := conf.Get("bootstrap.servers", "")
    assert.Equal(t, "localhost:9092", val)
}
```

### Integration Tests

**Approach:** Real Kafka cluster via Docker Compose

**Setup (`kafka/testresources/docker-compose.yml`):**

```yaml
version: '3'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  broker:
    image: confluentinc/cp-kafka:latest
    depends_on:
      - zookeeper
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
```

**Test:**

```go
// kafka/integration_test.go

func TestProducerConsumer(t *testing.T) {
    // Requires running Kafka cluster
    if testing.Short() {
        t.Skip("Skipping integration test")
    }

    producer, _ := NewProducer(&ConfigMap{
        "bootstrap.servers": "localhost:9092",
    })

    consumer, _ := NewConsumer(&ConfigMap{
        "bootstrap.servers": "localhost:9092",
        "group.id":          "test-group",
    })

    // Test produce -> consume flow
}
```

### Mock Cluster Testing

**Approach:** librdkafka's MockCluster (no Docker needed)

**Example (`kafka/integration_mock_test.go`):**

```go
func TestMockCluster(t *testing.T) {
    // Create mock cluster with 3 brokers
    mockCluster, _ := NewMockCluster(3)
    defer mockCluster.Close()

    producer, _ := NewProducer(&ConfigMap{
        "bootstrap.servers": mockCluster.BootstrapServers(),
    })

    // Produce to mock cluster
    producer.Produce(&Message{...}, nil)

    // Inject failures
    mockCluster.SetBrokerDown(0)
}
```

### Performance Benchmarks

**Example (`kafka/producer_performance_test.go`):**

```go
func BenchmarkProducer(b *testing.B) {
    producer, _ := NewProducer(&ConfigMap{
        "bootstrap.servers": "localhost:9092",
    })

    msg := &Message{
        TopicPartition: TopicPartition{Topic: &topic},
        Value:          []byte("test message"),
    }

    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        producer.Produce(msg, nil)
    }
}

// Run: go test -bench=BenchmarkProducer -benchmem
```

---

## Code Organization

### Platform-Specific Builds

**Problem:** librdkafka differs per platform (glibc vs musl, x64 vs ARM)

**Solution:** Build tags + platform-specific files

**Example: `kafka/build_glibc_linux_amd64.go`:**

```go
//go:build !dynamic && (linux && amd64) && !musl

package kafka

/*
#cgo CFLAGS: -I${SRCDIR}/librdkafka_vendor/v2.12.0_linux_amd64_glibc/include
#cgo LDFLAGS: ${SRCDIR}/librdkafka_vendor/v2.12.0_linux_amd64_glibc/librdkafka_vendor.a
*/
import "C"
```

**Build commands:**

```bash
# Default (glibc Linux x64)
go build ./...

# Alpine Linux (musl)
go build -tags musl ./...

# Dynamic linking (system librdkafka)
go build -tags dynamic ./...
```

### Directory Organization

```
kafka/
├── producer.go              # Producer implementation
├── consumer.go              # Consumer implementation
├── adminapi.go              # Admin operations
├── handle.go                # Shared infrastructure
├── message.go               # Message types
├── error.go                 # Error handling
├── config.go                # Configuration
│
├── build_*.go              # Platform-specific builds
│
├── librdkafka_vendor/      # Static libraries
│   ├── v2.12.0_linux_amd64_glibc/
│   ├── v2.12.0_darwin_arm64/
│   └── ...
│
└── testresources/          # Docker Compose for tests
    ├── docker-compose.yml
    └── docker-compose-kraft.yml
```

---

## Key Takeaways

1. **Handle pattern reduces duplication**: Shared infrastructure for all clients
2. **CGo requires discipline**: Always `defer C.free()`, handle nil slices carefully
3. **Error codes are generated**: Sync with librdkafka automatically
4. **Testing is multi-layered**: Unit → Mock → Integration → Performance
5. **Build tags enable multi-platform**: Same codebase, platform-specific linking

---

## What's Next?

**Part 4** explores extending and integrating confluent-kafka-go:
- Schema Registry integration (Avro, Protobuf, JSON Schema)
- Field-level encryption with KMS providers
- OAuth authentication
- Transactional workflows (EOS)

---

**Next in series:** [Part 4: Extending and Integrating](./04-extending-integrating.md)
