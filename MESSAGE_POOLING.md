# Message Pooling for High-Throughput Scenarios

## Overview

Message pooling is an opt-in performance optimization that reduces garbage collection (GC) pressure in high-throughput scenarios (>100K msg/sec) by reusing `Message` struct instances via `sync.Pool`.

## When to Use Message Pooling

### Use Cases

✅ **Enable pooling when:**
- Processing >100,000 messages per second
- Experiencing GC pressure (high GC pause times)
- Running latency-sensitive applications
- Memory allocation is a bottleneck

❌ **Don't enable pooling when:**
- Processing <100,000 messages per second (overhead may outweigh benefits)
- Messages are retained long-term (breaks pooling assumptions)
- Memory usage is not a concern

## Configuration

Enable message pooling by setting `go.message.pool.enable` to `true` when creating a Producer or Consumer.

### Producer Example

```go
producer, err := kafka.NewProducer(&kafka.ConfigMap{
    "bootstrap.servers":      "localhost:9092",
    "go.message.pool.enable": true,  // Enable message pooling
})
```

### Consumer Example

```go
consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
    "bootstrap.servers":      "localhost:9092",
    "group.id":               "myGroup",
    "go.message.pool.enable": true,  // Enable message pooling
})
```

## Performance Impact

### Expected Improvements

Based on benchmarks:

- **30-50% reduction** in memory allocations
- **12-15% faster** message processing
- **50% reduction** in GC pressure
- **Fewer, shorter** GC pauses

### Benchmark Results

```
BenchmarkMessageAllocWithoutPool-8    1000000   1200 ns/op   256 B/op   3 allocs/op
BenchmarkMessageAllocWithPool-8       1000000   1050 ns/op   128 B/op   1 allocs/op
```

## Best Practices

### Consumer

When consuming messages with pooling enabled:

1. **Process messages immediately** - Don't retain messages beyond the scope of processing
2. **Copy data if needed** - If you need to keep message data, copy it:

```go
msg := consumer.Poll(100)
if m, ok := msg.(*kafka.Message); ok {
    // Good: Copy data you need to keep
    value := make([]byte, len(m.Value))
    copy(value, m.Value)

    // Process m.Value immediately
    processMessage(m)

    // Bad: Don't retain the message
    // savedMessages = append(savedMessages, m) // DON'T DO THIS
}
```

### Producer

When producing with pooling enabled:

1. **Delivery reports** - Messages in delivery reports are pooled and should not be retained
2. **Disable delivery reports** - For maximum throughput, disable delivery reports if not needed:

```go
producer, err := kafka.NewProducer(&kafka.ConfigMap{
    "bootstrap.servers":      "localhost:9092",
    "go.message.pool.enable": true,
    "go.delivery.reports":    false,  // Disable for max throughput
})
```

## Important Notes

### Message Lifecycle

With pooling enabled, the message lifecycle is:

1. **Allocation** - Message is retrieved from pool (or allocated if pool is empty)
2. **Usage** - Message is populated with data and used
3. **Collection** - Message goes out of scope and becomes eligible for pooling
4. **Reset** - Before returning to pool, all references are cleared to prevent memory leaks

### Thread Safety

`sync.Pool` is thread-safe, so message pooling works correctly with concurrent producers/consumers.

### Memory Leaks Prevention

The pool implementation automatically clears all message fields before returning messages to the pool:

- `Key`, `Value`, `Headers` - Set to nil
- `TopicPartition.Topic` - Set to nil
- `Opaque` - Set to nil
- `LeaderEpoch` - Set to nil
- Numeric fields - Reset to zero values

## Troubleshooting

### High Memory Usage

If you're experiencing high memory usage with pooling enabled:

1. **Check message retention** - Ensure messages aren't being retained beyond processing
2. **Verify pool is enabled** - Check that `go.message.pool.enable` is set to `true`
3. **Monitor GC** - Use `GODEBUG=gctrace=1` to monitor GC behavior

### Unexpected Behavior

If you see unexpected message data:

1. **Message retention** - You may be retaining pooled messages. Make copies of data you need to keep.
2. **Concurrent access** - Ensure you're not accessing messages from multiple goroutines

## Performance Tuning

For maximum throughput with pooling:

### Producer Configuration

```go
producer, err := kafka.NewProducer(&kafka.ConfigMap{
    "bootstrap.servers":       "localhost:9092",
    "go.message.pool.enable":  true,
    "go.delivery.reports":     false,  // Disable if not needed
    "compression.type":        "lz4",
    "batch.size":              16384,
    "linger.ms":               10,
    "acks":                    1,
})
```

### Consumer Configuration

```go
consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
    "bootstrap.servers":           "localhost:9092",
    "group.id":                    "myGroup",
    "go.message.pool.enable":      true,
    "fetch.min.bytes":             1,
    "fetch.wait.max.ms":           100,
    "max.partition.fetch.bytes":   1048576,
})
```

## Examples

See the following examples for complete implementations:

- `examples/high_throughput_example/` - High-throughput producer with pooling
- `examples/high_throughput_consumer_example/` - High-throughput consumer with pooling

## Benchmarking

To benchmark pooling in your environment:

```bash
cd kafka
go test -bench=BenchmarkMessagePool -benchmem
```

## Related Configuration

- `go.delivery.reports` - Disable for maximum producer throughput
- `go.delivery.report.fields` - Minimize fields in delivery reports
- `go.events.channel.size` - Buffer size for events channel

## References

- [Go sync.Pool documentation](https://pkg.go.dev/sync#Pool)
- [librdkafka configuration](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
- RFC-0006: Object Pooling Implementation
