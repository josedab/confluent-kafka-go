# Repository Structure: confluent-kafka-go

**Analysis Date:** 2025-11-16
**Commit SHA:** `f8569d7adaae6575fff184a0b3e5c0cab025e19f`

## Overview

The confluent-kafka-go repository is organized as a **multi-module monorepo** with clear separation between core client libraries, examples, and testing utilities.

## Directory Tree

```
confluent-kafka-go/
├── .github/                    # GitHub Actions CI/CD workflows
│   └── workflows/
│       └── (CI configuration)
│
├── kafka/                      # 🔥 CORE: Main Kafka client library
│   ├── *.go                    # Core implementation (29 non-test files)
│   ├── *_test.go              # Unit and integration tests (31 files)
│   ├── go_rdkafka_generr/     # Error code generator utility
│   ├── librdkafka_vendor/     # 🔥 Bundled static libraries (~112MB)
│   │   ├── v2.12.0_darwin_amd64/
│   │   ├── v2.12.0_darwin_arm64/
│   │   ├── v2.12.0_linux_amd64_glibc/
│   │   ├── v2.12.0_linux_amd64_musl/
│   │   ├── v2.12.0_linux_arm64_glibc/
│   │   ├── v2.12.0_linux_arm64_musl/
│   │   └── v2.12.0_windows_amd64/
│   └── testresources/         # Docker Compose for local testing
│       ├── docker-compose-kraft.yml    # KRaft mode (ZooKeeper-less)
│       └── docker-compose.yml          # ZooKeeper mode
│
├── schemaregistry/            # 🔥 CORE: Schema Registry client
│   ├── *.go                   # Main client implementation
│   ├── cache/                 # Schema caching (LRU, map)
│   ├── confluent/             # Confluent-specific types
│   ├── internal/              # Internal REST service
│   ├── rest/                  # REST API client
│   ├── rules/                 # Data contract rules engine
│   │   ├── cel/              # Common Expression Language executor
│   │   ├── encryption/        # Field-level encryption
│   │   │   ├── awskms/       # AWS KMS integration
│   │   │   ├── azurekms/     # Azure Key Vault
│   │   │   ├── gcpkms/       # GCP KMS
│   │   │   ├── hcvault/      # HashiCorp Vault
│   │   │   └── localkms/     # Local test KMS
│   │   └── jsonata/          # JSONata transformation executor
│   ├── serde/                 # Serialization/deserialization framework
│   │   ├── avro/             # Legacy Avro v1 (heetch/avro)
│   │   ├── avrov2/           # Modern Avro v2 (hamba/avro)
│   │   ├── jsonschema/       # JSON Schema support
│   │   └── protobuf/         # Protocol Buffers support
│   └── test/                  # Test schemas and fixtures
│
├── examples/                  # 🔥 54+ comprehensive example programs
│   ├── go.mod                 # Separate module for examples
│   ├── admin_*/              # 19 admin API examples
│   │   ├── admin_create_topic/
│   │   ├── admin_delete_topics/
│   │   ├── admin_describe_cluster/
│   │   └── ... (16 more)
│   ├── *_producer_*/         # 9 producer examples
│   │   ├── producer_example/              # Basic producer
│   │   ├── idempotent_producer_example/   # Idempotent producer
│   │   ├── avrov2_producer_*/            # Avro examples
│   │   ├── json_producer_*/              # JSON examples
│   │   └── protobuf_producer_*/          # Protobuf examples
│   ├── *_consumer_*/         # 9 consumer examples
│   │   ├── consumer_example/              # Basic consumer
│   │   ├── cooperative_consumer_example/  # Cooperative rebalancing
│   │   ├── avrov2_consumer_*/            # Avro examples
│   │   └── ... (6 more)
│   ├── transactions_example/ # Exactly-once semantics (EOS) example
│   ├── go-kafkacat/          # kafkacat clone in Go
│   ├── confluent_cloud_example/ # Confluent Cloud integration
│   ├── oauthbearer_*/        # OAuth authentication examples
│   ├── mockcluster_*/        # Mock cluster testing examples
│   ├── stats_example/        # Statistics events example
│   └── legacy/               # Deprecated channel-based examples
│
├── kafkatest/                 # System test clients (separate module)
│   ├── go.mod
│   ├── verifiable_producer.go # Consistent test data generation
│   └── verifiable_consumer.go # Verification consumer
│
├── soaktest/                  # Performance and stress testing (separate module)
│   ├── go.mod
│   └── (soak test implementations)
│
├── mk/                        # Build system utilities
│   ├── Makefile              # Main build targets
│   └── (mingw-w64 support for Windows)
│
├── go.mod                     # 🔥 Main module definition
├── go.sum                     # Dependency checksums
├── README.md                  # Project README
├── LICENSE                    # Apache 2.0 license
└── (other config files)
```

## Module Organization

The repository uses **Go workspaces** with 5 independent modules:

### 1. Main Module: `/` (github.com/confluentinc/confluent-kafka-go/v2)
- **Packages**: `kafka`, `schemaregistry` (and sub-packages)
- **Purpose**: Core client library for production use
- **Dependencies**: 36 direct, ~200 transitive

### 2. Examples Module: `/examples`
- **Purpose**: Self-contained example programs
- **Dependencies**: Uses `replace` directive to reference main module locally
- **Usage**: `go run` individual example programs

### 3. Kafkatest Module: `/kafkatest`
- **Purpose**: Verifiable producer/consumer for system testing
- **Dependencies**: Minimal (kafka client only)
- **Usage**: Test harness for integration tests

### 4. Soaktest Module: `/soaktest`
- **Purpose**: Long-running performance tests
- **Dependencies**: kafka client + performance tooling
- **Usage**: Continuous performance validation

### 5. Lambda Example Module: `/examples/docker_aws_lambda_example`
- **Purpose**: AWS Lambda-specific example
- **Dependencies**: AWS SDK + kafka client
- **Usage**: Demonstrates serverless Kafka integration

## Package-Level Organization

### kafka Package (`/kafka`)

**Purpose**: Core Kafka client implementation

**Key Files** (by responsibility):

#### Entry Points (3 files)
- `producer.go` — Producer API and implementation
- `consumer.go` — Consumer API and implementation
- `adminapi.go` — AdminClient API (130KB, most complex file)

#### Core Types (6 files)
- `kafka.go` — Package documentation, TopicPartition, Node, UUID
- `message.go` — Message structure and headers
- `event.go` — Event types (AssignedPartitions, RevokedPartitions, etc.)
- `error.go` — Error types and handling
- `generated_errors.go` — Auto-generated error codes from librdkafka
- `offset.go` — Offset types and constants

#### Configuration & Metadata (3 files)
- `config.go` — ConfigMap for client configuration
- `metadata.go` — Cluster metadata structures
- `adminoptions.go` — Options for admin operations

#### Internal Infrastructure (7 files)
- `handle.go` — Common handle for Producer/Consumer/AdminClient
- `context.go` — Go context support
- `log.go` — Logging infrastructure
- `header.go` — Message header implementation
- `00version.go` — Version checks for librdkafka
- `mockcluster.go` — Mock Kafka cluster for testing
- `testhelpers_test.go` — Test utilities

#### Platform-Specific Build (8 files)
- `build_glibc_linux_amd64.go` — Linux x64 (glibc)
- `build_glibc_linux_arm64.go` — Linux ARM64 (glibc)
- `build_musl_linux_amd64.go` — Alpine Linux x64
- `build_musl_linux_arm64.go` — Alpine Linux ARM64
- `build_darwin_amd64.go` — macOS Intel
- `build_darwin_arm64.go` — macOS Apple Silicon
- `build_windows.go` — Windows x64
- `build_dynamic.go` — Dynamic linking mode

#### Testing (31 files)
- `integration_test.go` (112KB) — Comprehensive integration tests
- `txn_integration_test.go` — Transaction tests
- `consumer_test.go` / `producer_test.go` — Unit tests
- `*_performance_test.go` — Performance benchmarks
- Other component-specific test files

---

### schemaregistry Package (`/schemaregistry`)

**Purpose**: Schema Registry client and serialization framework

**Subpackage Organization**:

#### `/schemaregistry` (root)
**Key Files**:
- `schemaregistry_client.go` (54KB) — Main client implementation
- `mock_schemaregistry_client.go` (40KB) — Mock for testing
- `config.go` — Client configuration
- `rule*.go` — Data contract rule definitions

#### `/schemaregistry/cache`
**Purpose**: Schema caching implementations
- `cache.go` — Cache interface
- `lru_cache.go` — LRU cache implementation
- `map_cache.go` — Simple map cache

#### `/schemaregistry/serde`
**Purpose**: Serialization framework
- `serde.go` — Base Serializer/Deserializer interfaces
- `/avro/` — Legacy Avro v1 (heetch/avro)
- `/avrov2/` — Modern Avro v2 (hamba/avro) 🔥 **Recommended**
- `/jsonschema/` — JSON Schema support
- `/protobuf/` — Protocol Buffers support

#### `/schemaregistry/rules`
**Purpose**: Data contract rules engine
- `executor.go` — Rule execution framework
- `/cel/` — Common Expression Language rules
- `/jsonata/` — JSONata transformation rules
- `/encryption/` — Field-level encryption
  - `/awskms/` — AWS KMS driver
  - `/azurekms/` — Azure Key Vault driver
  - `/gcpkms/` — GCP KMS driver
  - `/hcvault/` — HashiCorp Vault driver
  - `/localkms/` — Local test KMS

#### `/schemaregistry/internal`
**Purpose**: Internal REST service implementation
- HTTP client wrapper
- Authentication (Basic, Bearer, mTLS)
- Request/response handling

---

## Examples Organization (`/examples`)

### By Category:

#### Admin API Examples (19 examples)
Operations: create/delete topics, ACLs, consumer groups, configs, etc.

**Pattern**: All follow consistent structure:
```
admin_<operation>_<resource>/
└── admin_<operation>_<resource>.go
```

**Examples**:
- `admin_create_topic` — Create topics
- `admin_delete_acls` — Delete ACLs
- `admin_describe_cluster` — Cluster metadata
- `admin_elect_leaders` — Leader election
- `admin_list_offsets` — Offset management

#### Producer Examples (9 examples)
**Basic**:
- `producer_example` — Minimal async producer
- `producer_custom_channel_example` — Custom delivery report channel
- `idempotent_producer_example` — Idempotent producer

**With Serialization**:
- `avrov2_producer_example` — Avro v2 serialization
- `json_producer_example` — JSON Schema
- `protobuf_producer_example` — Protocol Buffers

**With Encryption**:
- `*_producer_encryption_example` — Field-level encryption

**Migration**:
- `avrov2_producer_migration_example` — Avro v1 → v2 migration

#### Consumer Examples (9 examples)
**Basic**:
- `consumer_example` — Minimal poll-based consumer
- `consumer_rebalance_example` — Manual partition assignment
- `consumer_offset_metadata` — Custom offset metadata
- `cooperative_consumer_example` — Cooperative rebalancing

**With Deserialization**:
- `avrov2_consumer_example` — Avro v2
- `json_consumer_example` — JSON Schema
- `protobuf_consumer_example` — Protocol Buffers

**With Decryption**:
- `*_consumer_encryption_example` — Field-level decryption

#### Special Purpose Examples
- `transactions_example` — Full EOS (Exactly-Once Semantics) demo
- `go-kafkacat` — kafkacat clone with producer/consumer modes
- `confluent_cloud_example` — Confluent Cloud integration
- `oauthbearer_*` — OAuth authentication (3 examples)
- `mockcluster_*` — Testing with mock clusters (2 examples)
- `stats_example` — Statistics events handling
- `library-version` — Version information
- `docker_aws_lambda_example` — AWS Lambda integration
- `schema_registry_bearer_authentication` — SR auth example

---

## File Naming Conventions

### Go Source Files
- `<feature>.go` — Implementation
- `<feature>_test.go` — Unit/integration tests
- `<feature>_performance_test.go` — Benchmarks
- `build_<platform>.go` — Platform-specific builds
- `generated_*.go` — Auto-generated code

### Build Tags
Files use build tags for platform selection:
```go
//go:build !dynamic && (linux && amd64)
// +build !dynamic,linux,amd64
```

**Tags**:
- `dynamic` — Use system librdkafka (vs. bundled static)
- `musl` — musl libc (Alpine Linux) vs. glibc
- `linux`, `darwin`, `windows` — OS selection
- `amd64`, `arm64` — Architecture selection

---

## Configuration Files

### Root Level
- `go.mod` / `go.sum` — Go module definition
- `README.md` — Project documentation
- `LICENSE` — Apache 2.0 license
- `.gitignore` — Git ignore patterns

### CI/CD
- `.github/workflows/*.yml` — GitHub Actions workflows

### Testing
- `kafka/testresources/docker-compose*.yml` — Test Kafka clusters
- `kafka/testresources/testconf.json` — Integration test configuration

---

## Important Paths for Development

### Adding a New Feature
1. **Implementation**: `kafka/<feature>.go` or `schemaregistry/<feature>.go`
2. **Tests**: `kafka/<feature>_test.go`
3. **Example**: `examples/<feature>_example/<feature>_example.go`
4. **Documentation**: Update `README.md`, add example to docs

### Running Tests
```bash
cd kafka/
# Unit tests
go test -v

# Integration tests (requires Docker)
cd testresources/
docker-compose up -d
cd ..
go test -v -tags integration

# Performance tests
go test -v -bench=. -run=^$ -benchmem
```

### Building for Different Platforms
```bash
# Default (bundled static library)
go build ./...

# Alpine Linux
go build -tags musl ./...

# Dynamic linking (system librdkafka)
go build -tags dynamic ./...
```

---

## Dependency Vendoring

### librdkafka Static Libraries
Located in `kafka/librdkafka_vendor/v2.12.0_<platform>/`

**Contents per platform**:
- `librdkafka_vendor.a` — Static library (~20-30MB compressed)
- `include/` — C header files

**Total size**: ~112MB across all platforms

**Why bundled?**
- Eliminates installation complexity
- Guarantees version compatibility
- Enables `go get` to work out-of-the-box

---

## Test Organization

### Unit Tests
- Located alongside implementation files
- Filename pattern: `*_test.go`
- Run with: `go test`

### Integration Tests
- `kafka/integration_test.go` (112KB, most comprehensive)
- `kafka/txn_integration_test.go` — Transaction tests
- Require: Docker Compose test clusters
- Run with: `go test -tags integration`

### Performance Tests
- `kafka/consumer_performance_test.go`
- `kafka/producer_performance_test.go`
- Run with: `go test -bench=. -benchmem`

### System Tests
- `kafkatest/` module
- Verifiable producer/consumer for end-to-end testing

### Soak Tests
- `soaktest/` module
- Long-running stability and performance tests

---

## Navigation Tips

### Finding Feature Implementation
1. **Producer feature**: Search `kafka/producer.go`
2. **Consumer feature**: Search `kafka/consumer.go`
3. **Admin operation**: Search `kafka/adminapi.go`
4. **Schema Registry**: Search `schemaregistry/schemaregistry_client.go`
5. **Serialization**: Check `schemaregistry/serde/<format>/`

### Understanding CGo Boundaries
1. **Import "C"**: Marks CGo boundary
2. **C.*** calls**: C function invocations
3. **C.CString()**: Go string → C string (requires `C.free()`)
4. **C.GoString()**: C string → Go string
5. **unsafe.Pointer()**: Type conversion between Go/C

### Error Handling Flow
1. **C errors**: Converted by `newError()` or `newErrorFromCErrorDestroy()`
2. **Error types**: Check `kafka/error.go`, `kafka/generated_errors.go`
3. **Error handling**: See package documentation in `kafka/kafka.go`

---

## Summary

The confluent-kafka-go repository is well-organized with:
- ✅ Clear separation of concerns (core lib vs. examples vs. tests)
- ✅ Multi-module structure for independent versioning
- ✅ Comprehensive examples for every feature
- ✅ Platform-specific build organization
- ✅ Bundled dependencies for ease of use

**Complexity Hotspots**:
- `kafka/adminapi.go` (130KB) — Most complex file
- `kafka/integration_test.go` (112KB) — Most comprehensive tests
- `schemaregistry/` package — Most dependencies

**Recommended Entry Points**:
- Start: `examples/producer_example/`, `examples/consumer_example/`
- Understand: `kafka/kafka.go` (package documentation)
- Deep dive: `kafka/handle.go` (core abstraction)
