# Schema Registry Deep Dive: Serialization, Evolution, Encryption

**Part 6 of 6** - **Final Part**
**Reading Time:** ~11 minutes

## Topics Covered

- Schema Registry client architecture
- Schema evolution (backward, forward, full compatibility)
- Serde implementations (Avro v1 vs v2, Protobuf, JSON Schema)
- Field-level encryption with Tink
- Data contract rules (CEL, JSONata)

## Schema Registry Architecture

### Client Components

```
┌────────────────────────────────────────┐
│  Application                            │
└────────────┬───────────────────────────┘
             │
             ↓
┌────────────────────────────────────────┐
│  Serializer/Deserializer               │
│  - Format-specific (Avro, Protobuf)    │
│  - Caches schemas locally              │
└────────────┬───────────────────────────┘
             │
             ↓
┌────────────────────────────────────────┐
│  Schema Registry Client                │
│  - REST API wrapper                    │
│  - LRU cache (default: 1000 schemas)   │
└────────────┬───────────────────────────┘
             │
             ↓
┌────────────────────────────────────────┐
│  Schema Registry (HTTP Server)         │
│  - Schema storage                      │
│  - Compatibility checking              │
└────────────────────────────────────────┘
```

## Schema Evolution

### Compatibility Modes

**BACKWARD** (default): New schema can read old data
```
v1: {"name": "string"}
v2: {"name": "string", "email": "string" = "default@example.com"}
     ✅ Can read v1 data (email uses default)
```

**FORWARD**: Old schema can read new data
```
v1: {"name": "string", "email": "string"}
v2: {"name": "string"}  // Dropped email field
     ✅ v1 readers ignore missing email
```

**FULL**: Both backward and forward compatible

**NONE**: No compatibility checks

### Wire Format

```
[Magic Byte: 0x00] [Schema ID: 4 bytes] [Avro Binary Data: N bytes]
```

**Example:**
```
0x00 0x00 0x00 0x00 0x05 0x0A 0x41 0x6C 0x69 0x63 0x65 ...
 ^    ^^^^^^^^^^^^^^    ^^^^^^^^^^^^^^^^^^^^^^^^^^
Magic     Schema ID=5        Avro data (name="Alice")
```

## Serde Implementations

### Avro v2 (Recommended)

```go
import "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avrov2"

// Generic (schema-driven)
serializer, _ := avrov2.NewSerializer(client, serde.ValueSerde, avrov2.NewSerializerConfig())
bytes, _ := serializer.Serialize("topic", map[string]interface{}{"name": "Alice"})

// Specific (Go struct)
type User struct {
    Name  string `avro:"name"`
    Email string `avro:"email"`
}
bytes, _ := serializer.Serialize("topic", &User{Name: "Alice", Email: "alice@example.com"})
```

### Protobuf

```go
import "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/protobuf"

// Generate Go code from .proto
// protoc --go_out=. user.proto

serializer, _ := protobuf.NewSerializer(client, serde.ValueSerde, protobuf.NewSerializerConfig())
bytes, _ := serializer.Serialize("topic", &pb.User{Name: "Alice"})
```

## Encryption

### Tink Integration

confluent-kafka-go uses Google's Tink for cryptography:
- FIPS 140-2 validated
- Misuse-resistant API
- Multiple KMS backends

### Field-Level Encryption

```go
rule := &schemaregistry.Rule{
    Name: "encryptPII",
    Kind: "TRANSFORM",
    Mode: "WRITEREAD",
    Type: "ENCRYPT",
    Tags: []string{"PII"},
    Params: map[string]string{
        "kms.type":   "aws-kms",
        "kms.key.id": "arn:aws:kms:...",
        "encrypt.fields": "email,ssn",
    },
}

// Schema with encryption rule
schema := &schemaregistry.SchemaInfo{
    Schema: avroSchema,
    RuleSet: &schemaregistry.RuleSet{
        DomainRules: []schemaregistry.Rule{*rule},
    },
}

// Encryption happens automatically during serialization
bytes, _ := serializer.Serialize("topic", &User{
    Name:  "Alice",         // Not encrypted
    Email: "alice@example.com",  // Encrypted (tagged PII)
    SSN:   "123-45-6789",   // Encrypted (tagged PII)
})
```

## Data Contract Rules

### CEL Validation

```go
rule := &schemaregistry.Rule{
    Name: "validateAge",
    Kind: "CONDITION",
    Type: "CEL",
    Mode: "WRITE",
    Expr: "message.age >= 18 && message.age < 120",
}

// Serialization fails if rule violated
bytes, err := serializer.Serialize("topic", &User{Age: 15})
// err: "validation failed: age must be >= 18"
```

### JSONata Transformation

```go
rule := &schemaregistry.Rule{
    Name: "normalizeEmail",
    Kind: "TRANSFORM",
    Type: "JSONATA",
    Mode: "WRITE",
    Expr: "$merge([message, {'email': $lowercase(message.email)}])",
}

// Email automatically lowercased before serialization
serializer.Serialize("topic", &User{Email: "Alice@Example.COM"})
// Result: email = "alice@example.com"
```

---

## Series Conclusion

We've explored confluent-kafka-go from architecture to advanced features:

1. **Part 1:** Three-tier architecture, CGo integration, Handle pattern
2. **Part 2:** Producer/Consumer internals, rebalancing, error handling
3. **Part 3:** Patterns and practices, testing, build system
4. **Part 4:** Schema Registry, encryption, OAuth, transactions
5. **Part 5:** Performance analysis, tuning, scaling
6. **Part 6:** Schema evolution, Serde, encryption, rules

**Key Takeaways:**
- CGo wrapper provides proven reliability (librdkafka)
- Performance is excellent (>800K msg/sec per producer)
- Schema Registry integration is best-in-class
- Enterprise features (encryption, transactions) are production-ready

**What's Next?**
- Try the examples: `/examples/`
- Read the RFCs: Improvement proposals for v3.0
- Contribute: The codebase is well-organized and contributor-friendly

---

**Thank you for reading this series!** Questions or feedback? File issues on GitHub or reach out to maintainers.
