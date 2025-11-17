# Extending and Integrating confluent-kafka-go

**Part 4 of 6** - **Schema Registry, Encryption, Auth, Transactions**
**Reading Time:** ~12 minutes

## Topics Covered

- Schema Registry architecture and subject naming
- Serde framework (Avro v1/v2, Protobuf, JSON Schema)
- Field-level encryption (AWS KMS, GCP KMS, Azure, Vault)
- Data contract rules (CEL validation, JSONata transformation)
- OAuth/SASL authentication patterns
- Transactional workflows (exactly-once semantics)

## Schema Registry Integration

### Client Architecture

```go
import "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"

client, _ := schemaregistry.NewClient(schemaregistry.NewConfig("http://localhost:8081"))

// Register schema
schema := `{"type":"record","name":"User","fields":[{"name":"name","type":"string"}]}`
id, _ := client.Register("users-value", schema, schemaregistry.Avro)

// Retrieve schema
info, _ := client.GetLatestSchema("users-value")
```

### Serde Framework

**Avro v2 (Recommended):**

```go
import "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avrov2"

type User struct {
    Name  string `avro:"name"`
    Email string `avro:"email"`
}

serializer, _ := avrov2.NewSerializer(client, serde.ValueSerde, avrov2.NewSerializerConfig())

// Serialize
bytes, _ := serializer.Serialize("users", &User{Name: "Alice", Email: "alice@example.com"})

// Deserialize
deserializer, _ := avrov2.NewDeserializer(client, serde.ValueSerde, avrov2.NewDeserializerConfig())
var user User
deserializer.DeserializeInto("users", bytes, &user)
```

### Field-Level Encryption

```go
import "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption/awskms"

rule := &schemaregistry.Rule{
    Name: "encryptPII",
    Kind: "TRANSFORM",
    Mode: "WRITEREAD",
    Type: "ENCRYPT",
    Tags: []string{"PII"},
    Params: map[string]string{
        "kms.type":   "aws-kms",
        "kms.key.id": "arn:aws:kms:us-east-1:...",
    },
}

// Schema with rule
schema := &schemaregistry.SchemaInfo{
    Schema:   avroSchema,
    RuleSet:  &schemaregistry.RuleSet{DomainRules: []schemaregistry.Rule{*rule}},
}
```

### OAuth Authentication

```go
consumer, _ := kafka.NewConsumer(&kafka.ConfigMap{
    "bootstrap.servers":      "localhost:9092",
    "security.protocol":      "SASL_SSL",
    "sasl.mechanism":         "OAUTHBEARER",
    "sasl.oauthbearer.method": "oidc",
    "sasl.oauthbearer.client.id": "my-client-id",
    "sasl.oauthbearer.client.secret": "my-secret",
    "sasl.oauthbearer.token.endpoint.url": "https://auth.example.com/token",
})
```

### Transactions (EOS)

```go
producer, _ := kafka.NewProducer(&kafka.ConfigMap{
    "bootstrap.servers": "localhost:9092",
    "transactional.id":  "my-tx-producer",
})

producer.InitTransactions(nil)

producer.BeginTransaction()
producer.Produce(&kafka.Message{...}, nil)
producer.SendOffsetsToTransaction(offsets, groupMetadata, nil)
producer.CommitTransaction(nil)
```

---

**Key Takeaways:**
- Schema Registry enables schema evolution and compatibility
- 4 serde formats supported (Avro v1/v2, Protobuf, JSON Schema)
- Field-level encryption integrates with major cloud KMS providers
- Transactions provide exactly-once semantics

**Next:** [Part 5: Performance Analysis](./05-performance-analysis.md)
