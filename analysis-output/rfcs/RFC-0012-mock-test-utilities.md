# RFC-0012: Comprehensive Mock/Test Utilities

**Status:** Draft 📝
**Author:** Technical Analysis Team
**Created:** 2025-11-20
**Priority:** P0 (Critical - Developer Experience)

## Summary

Provide official mock Producer, Consumer, and AdminClient implementations to enable unit testing without running a real Kafka cluster, dramatically improving developer experience and test speed.

## Motivation

### Current State

No official mocking support:

```go
func TestMyService(t *testing.T) {
    // Problem: Must run real Kafka or write complex mocks
    producer, _ := kafka.NewProducer(&kafka.ConfigMap{
        "bootstrap.servers": "localhost:9092", // Requires Kafka!
    })

    service := NewMyService(producer)
    // Test requires Kafka to be running
}
```

### Problems

1. **Slow Tests**: Integration tests require Kafka cluster (~5-10s per test)
2. **Flaky Tests**: Network issues, port conflicts, broker startup races
3. **CI Complexity**: Must run Kafka in CI (Docker, resources)
4. **High Barrier**: New contributors struggle with test setup
5. **No Isolation**: Tests can interfere with each other

### User Pain Points

From GitHub issues and Stack Overflow:
- "How do I unit test my Kafka code?"
- "Tests fail on CI but work locally"
- "Can't test edge cases (network failures, broker errors)"
- "Test suite takes 10 minutes to run"

## Detailed Design

### Mock Producer

```go
package kafkatest

// MockProducer records produced messages for assertions
type MockProducer struct {
    messages  []*kafka.Message
    errors    map[string]error // topic -> error to return
    delivered chan kafka.Event
    closed    bool
}

// NewMockProducer creates a mock producer
func NewMockProducer() *MockProducer {
    return &MockProducer{
        messages:  make([]*kafka.Message, 0),
        errors:    make(map[string]error),
        delivered: make(chan kafka.Event, 100),
    }
}

// Produce records the message and optionally returns error
func (m *MockProducer) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
    if m.closed {
        return kafka.NewError(kafka.ErrState, "producer closed", false)
    }

    // Check if error is configured for this topic
    if err, ok := m.errors[*msg.TopicPartition.Topic]; ok {
        return err
    }

    // Record message
    m.messages = append(m.messages, msg)

    // Send delivery report
    if deliveryChan != nil {
        deliveryChan <- msg
    } else {
        m.delivered <- msg
    }

    return nil
}

// Messages returns all produced messages
func (m *MockProducer) Messages() []*kafka.Message {
    return m.messages
}

// MessagesForTopic returns messages for specific topic
func (m *MockProducer) MessagesForTopic(topic string) []*kafka.Message {
    var msgs []*kafka.Message
    for _, msg := range m.messages {
        if *msg.TopicPartition.Topic == topic {
            msgs = append(msgs, msg)
        }
    }
    return msgs
}

// SetError configures error to return for topic
func (m *MockProducer) SetError(topic string, err error) {
    m.errors[topic] = err
}

// Events returns the events channel
func (m *MockProducer) Events() chan kafka.Event {
    return m.delivered
}

// Close closes the mock producer
func (m *MockProducer) Close() {
    m.closed = true
    close(m.delivered)
}

// Clear clears recorded messages
func (m *MockProducer) Clear() {
    m.messages = m.messages[:0]
}
```

### Mock Consumer

```go
// MockConsumer provides messages from a queue for testing
type MockConsumer struct {
    messages   chan *kafka.Message
    subscribed []string
    errors     chan error
    closed     bool
}

// NewMockConsumer creates a mock consumer
func NewMockConsumer() *MockConsumer {
    return &MockConsumer{
        messages: make(chan *kafka.Message, 100),
        errors:   make(chan error, 10),
    }
}

// AddMessage adds a message to be consumed
func (m *MockConsumer) AddMessage(msg *kafka.Message) {
    m.messages <- msg
}

// AddMessages adds multiple messages
func (m *MockConsumer) AddMessages(msgs []*kafka.Message) {
    for _, msg := range msgs {
        m.messages <- msg
    }
}

// AddError adds an error to be returned
func (m *MockConsumer) AddError(err error) {
    m.errors <- err
}

// SubscribeTopics records subscription
func (m *MockConsumer) SubscribeTopics(topics []string, rebalanceCb kafka.RebalanceCb) error {
    m.subscribed = topics
    return nil
}

// ReadMessage returns next message or error
func (m *MockConsumer) ReadMessage(timeout time.Duration) (*kafka.Message, error) {
    if m.closed {
        return nil, kafka.NewError(kafka.ErrState, "consumer closed", false)
    }

    select {
    case msg := <-m.messages:
        return msg, nil
    case err := <-m.errors:
        return nil, err
    case <-time.After(timeout):
        return nil, kafka.NewError(kafka.ErrTimedOut, "timed out", false)
    }
}

// Close closes the consumer
func (m *MockConsumer) Close() error {
    m.closed = true
    return nil
}

// SubscribedTopics returns topics consumer is subscribed to
func (m *MockConsumer) SubscribedTopics() []string {
    return m.subscribed
}
```

### Mock AdminClient

```go
// MockAdminClient simulates admin operations
type MockAdminClient struct {
    topics map[string]kafka.TopicSpecification
    errors map[string]error
}

// NewMockAdminClient creates mock admin client
func NewMockAdminClient() *MockAdminClient {
    return &MockAdminClient{
        topics: make(map[string]kafka.TopicSpecification),
        errors: make(map[string]error),
    }
}

// CreateTopics simulates topic creation
func (m *MockAdminClient) CreateTopics(ctx context.Context, topics []kafka.TopicSpecification, options ...kafka.CreateTopicsAdminOption) ([]kafka.TopicResult, error) {
    results := make([]kafka.TopicResult, len(topics))

    for i, topic := range topics {
        if err, ok := m.errors[topic.Topic]; ok {
            results[i] = kafka.TopicResult{
                Topic: topic.Topic,
                Error: kafka.NewError(kafka.ErrTopicAlreadyExists, err.Error(), false),
            }
        } else {
            m.topics[topic.Topic] = topic
            results[i] = kafka.TopicResult{Topic: topic.Topic}
        }
    }

    return results, nil
}

// SetError configures error for specific topic
func (m *MockAdminClient) SetError(topic string, err error) {
    m.errors[topic] = err
}
```

## Usage Examples

### Testing Producer Code

```go
func TestMessageProducer(t *testing.T) {
    // Create mock producer
    producer := kafkatest.NewMockProducer()
    defer producer.Close()

    // Test your service
    service := NewOrderService(producer)
    err := service.CreateOrder(Order{ID: "123", Amount: 100})

    // Assert
    assert.NoError(t, err)
    msgs := producer.MessagesForTopic("orders")
    assert.Len(t, msgs, 1)
    assert.Equal(t, []byte("123"), msgs[0].Key)
}

func TestProducerError(t *testing.T) {
    producer := kafkatest.NewMockProducer()
    producer.SetError("orders", errors.New("broker unavailable"))

    service := NewOrderService(producer)
    err := service.CreateOrder(Order{ID: "123"})

    // Should handle error gracefully
    assert.Error(t, err)
}
```

### Testing Consumer Code

```go
func TestMessageConsumer(t *testing.T) {
    consumer := kafkatest.NewMockConsumer()
    defer consumer.Close()

    // Add test messages
    topic := "orders"
    consumer.AddMessage(&kafka.Message{
        TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0},
        Value:          []byte(`{"id":"123","amount":100}`),
    })

    // Test your consumer
    processor := NewOrderProcessor(consumer)
    err := processor.ProcessNext()

    assert.NoError(t, err)
    // Assert side effects
}
```

## Implementation Plan

**Week 1: Core Mocks**
- Implement MockProducer
- Implement MockConsumer
- Basic test coverage

**Week 2: Advanced Features**
- MockAdminClient
- Error injection
- Message ordering

**Week 3: Documentation & Examples**
- Testing guide
- Migration examples
- Best practices

## Benefits

1. **Fast Tests**: Unit tests run in milliseconds (vs seconds)
2. **Reliable Tests**: No network dependencies, no flaky tests
3. **Easy CI**: No Kafka required in CI
4. **Edge Cases**: Test network failures, broker errors easily
5. **Better Coverage**: Can test error paths that are hard to trigger

## Success Criteria

- ✅ MockProducer/Consumer/AdminClient fully functional
- ✅ 100% of examples include test examples
- ✅ Documentation on testing strategies
- ✅ No breaking changes

**Effort:** 2-3 weeks
**Impact:** Very High (developer experience, test speed)
