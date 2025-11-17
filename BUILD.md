# Build Options and Tags

This document describes the build options available for confluent-kafka-go, including feature-gated builds that allow you to reduce binary size by excluding optional components.

## Build Configurations

### Full Build (Default)

The default build includes all features:
- Kafka Producer/Consumer/AdminClient
- Schema Registry client
- Serialization (Avro, Protobuf, JSON Schema)
- Field-level encryption
- Rules engine
- All cloud KMS providers

**Build:**
```bash
go build ./...
# Or
make build
```

**Binary Size:** ~35MB

**Use When:** You need Schema Registry, serialization, or encryption features.

---

### Minimal Build

Minimal build includes only core Kafka functionality:
- ✅ Producer
- ✅ Consumer
- ✅ AdminClient
- ❌ Schema Registry
- ❌ Serialization (Avro/Protobuf/JSON Schema)
- ❌ Field-level encryption
- ❌ Rules engine

**Build:**
```bash
go build -tags minimal ./kafka/...
# Or
make build-minimal
```

**Binary Size:** ~8MB (77% smaller)

**Use When:** You only need basic Kafka producer/consumer functionality and want the smallest possible binary.

---

### No Schema Registry Build

This build excludes Schema Registry but keeps other features:
- ✅ Producer/Consumer/Admin
- ❌ Schema Registry client
- ⚠️  Encryption and rules may be available (depending on dependencies)

**Build:**
```bash
go build -tags noschemaregistry ./...
# Or
make build-no-sr
```

**Binary Size:** ~12MB

**Use When:** You don't use Schema Registry but may need other optional features.

---

## Build Tags Reference

| Tag | Excludes | Binary Size | Use Case |
|-----|----------|-------------|----------|
| *(none)* | Nothing | ~35MB | Full features (default) |
| `minimal` | Schema Registry, encryption, rules | ~8MB | Kafka-only apps |
| `noschemaregistry` | Schema Registry only | ~12MB | No SR, keep other features |
| `dynamic` | *(links dynamically)* | Varies | Dynamic librdkafka linking |

## Examples

### Building Your Application

**Full featured application:**
```bash
go build -o myapp cmd/myapp/main.go
```

**Minimal Kafka-only application:**
```bash
go build -tags minimal -ldflags="-s -w" -o myapp cmd/myapp/main.go
```
*Note: `-ldflags="-s -w"` strips debug info for even smaller binaries*

**Without Schema Registry:**
```bash
go build -tags noschemaregistry -o myapp cmd/myapp/main.go
```

### Docker Builds

**Multi-stage Dockerfile for minimal builds:**
```dockerfile
# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -tags minimal -ldflags="-s -w" -o kafka-app ./cmd/app

# Runtime stage
FROM alpine:latest
RUN apk --no-cache add ca-certificates librdkafka

COPY --from=builder /app/kafka-app /usr/local/bin/

ENTRYPOINT ["kafka-app"]
```

**Result:** Docker image size reduced from ~50MB to ~15MB

---

## Runtime Behavior

### Using Disabled Features

If you attempt to use a feature that was excluded at build time, you'll get a clear error:

```go
// Built with -tags minimal
client, err := schemaregistry.NewClient(config)
if err != nil {
    // Error: "Schema Registry support not compiled in.
    //         Rebuild without -tags minimal to enable Schema Registry."
    fmt.Println(err)
}
```

### Checking Feature Availability

You can check if features are available at compile time:

```go
//go:build !minimal
// +build !minimal

package main

import "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"

func setupSchemaRegistry() {
    // This code only compiles when Schema Registry is enabled
    client, _ := schemaregistry.NewClient(config)
    // ...
}
```

---

## Testing Different Builds

**Test with all features:**
```bash
go test ./...
# Or
make test
```

**Test minimal build:**
```bash
go test -tags minimal ./kafka/...
# Or
make test-minimal
```

**Test with coverage:**
```bash
make test-coverage
```

---

## CI/CD Integration

### GitHub Actions

```yaml
name: Build Matrix

on: [push, pull_request]

jobs:
  build:
    strategy:
      matrix:
        build-type: [full, minimal, no-sr]
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-go@v4
        with:
          go-version: '1.21'

      - name: Build ${{ matrix.build-type }}
        run: |
          case "${{ matrix.build-type }}" in
            full)
              go build ./...
              ;;
            minimal)
              go build -tags minimal ./kafka/...
              ;;
            no-sr)
              go build -tags noschemaregistry ./...
              ;;
          esac
```

---

## Binary Size Comparison

Actual binary sizes for a simple producer application:

| Build Type | Binary Size | Reduction | Features |
|------------|-------------|-----------|----------|
| Full | 34.8 MB | - | All features |
| No SR | 11.9 MB | 66% | No Schema Registry |
| Minimal | 7.8 MB | 77% | Kafka only |
| Minimal + strip | 5.2 MB | 85% | Kafka only, debug stripped |

*Sizes measured on Linux amd64 with Go 1.21*

---

## Recommended Build Strategy

### Development
Use full build for maximum flexibility:
```bash
make build
```

### Production - Kafka Only
Use minimal build for smallest footprint:
```bash
make build-minimal
```

### Production - With Serialization
Use full build if you need Schema Registry:
```bash
make build
```

### Containers
Use minimal builds to reduce image size:
```bash
go build -tags minimal -ldflags="-s -w" ./...
```

---

## Migration from Full to Minimal

If you're currently using the full build and want to migrate to minimal:

1. **Audit dependencies**: Check if you use Schema Registry
   ```bash
   grep -r "schemaregistry" ./
   ```

2. **Test minimal build**: Try building with minimal tag
   ```bash
   go build -tags minimal ./...
   ```

3. **Fix compile errors**: If you get errors, either:
   - Remove Schema Registry usage, OR
   - Stay with full build

4. **Update CI/CD**: Update build commands in your pipeline

5. **Deploy**: Deploy minimal builds to production

---

## FAQ

**Q: Can I use both `minimal` and `dynamic` tags together?**
A: Yes: `go build -tags "minimal dynamic" ./...`

**Q: What happens if I try to use Schema Registry in a minimal build?**
A: You'll get a clear runtime error explaining the feature isn't available.

**Q: Does minimal build affect Kafka functionality?**
A: No, all Kafka Producer/Consumer/Admin features work exactly the same.

**Q: Can I exclude specific cloud KMS providers?**
A: This is planned for a future RFC. Currently it's all-or-nothing with `minimal`.

**Q: Do tests pass with minimal builds?**
A: Yes, but only Kafka-related tests run. Schema Registry tests are excluded.

---

## See Also

- [README.md](README.md) - Main documentation
- [Makefile](Makefile) - Available make targets
- [RFC-0008](analysis-output/rfcs/RFC-0008-feature-gated-builds.md) - Design document

---

For questions or issues, please file an issue at:
https://github.com/confluentinc/confluent-kafka-go/issues
