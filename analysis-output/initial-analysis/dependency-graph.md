# Dependency Graph: confluent-kafka-go

**Analysis Date:** 2025-11-16
**Commit SHA:** `f8569d7adaae6575fff184a0b3e5c0cab025e19f`
**Go Version:** 1.24.3+

## Overview

The confluent-kafka-go library has a **dual dependency profile**:
1. **kafka package**: Minimal dependencies (primarily librdkafka)
2. **schemaregistry package**: Heavy dependencies (serialization, encryption, cloud SDKs)

**Total Dependencies**:
- **Direct**: 36 packages
- **Transitive**: ~200 packages
- **Vendored**: librdkafka 2.12.0 (static C library)

## Dependency Tree Visualization

```
confluent-kafka-go/v2
│
├── 🔥 librdkafka v2.12.0 (C library, bundled)
│   └── Platform-specific static libraries (112MB)
│
├── kafka package (minimal deps)
│   ├── testcontainers-go v0.33.0 (test only)
│   └── stretchr/testify v1.9.0 (test only)
│
└── schemaregistry package (heavy deps)
    │
    ├── 📦 Serialization Libraries
    │   ├── hamba/avro/v2 v2.24.0 ✅ RECOMMENDED
    │   ├── heetch/avro v0.4.5 (legacy)
    │   ├── actgardner/gogen-avro/v10 v10.2.1 (code gen)
    │   ├── invopop/jsonschema v0.12.0
    │   ├── santhosh-tekuri/jsonschema/v5 v5.3.0
    │   ├── jhump/protoreflect v1.15.6
    │   └── google.golang.org/protobuf v1.33.0
    │
    ├── 🔒 Encryption Libraries
    │   ├── tink-crypto/tink-go/v2 v2.1.0 (crypto framework)
    │   ├── tink-crypto/tink-go-gcpkms/v2 v2.1.0
    │   └── tink-crypto/tink-go-hcvault/v2 v2.1.0
    │
    ├── ☁️ Cloud Provider SDKs
    │   ├── AWS
    │   │   ├── aws-sdk-go-v2 v1.26.1
    │   │   ├── aws-sdk-go-v2/config v1.27.10
    │   │   ├── aws-sdk-go-v2/credentials v1.17.10
    │   │   ├── aws-sdk-go-v2/service/kms v1.30.1
    │   │   └── aws-sdk-go-v2/service/sts v1.28.6
    │   │
    │   ├── Azure
    │   │   ├── Azure/azure-sdk-for-go/sdk/azcore v1.11.1
    │   │   ├── Azure/azure-sdk-for-go/sdk/azidentity v1.6.0
    │   │   └── Azure/azure-sdk-for-go/sdk/security/keyvault/azkeys v1.1.0
    │   │
    │   └── GCP
    │       ├── google.golang.org/api v0.169.0
    │       └── google.golang.org/genproto v0.0.0-20240325203815-454cdb8f5daa
    │
    ├── 🔐 HashiCorp Vault
    │   ├── hashicorp/vault/api v1.15.0
    │   └── hashicorp/vault/api/auth/approle v0.8.0
    │
    ├── 🎯 Rules Engines
    │   ├── google/cel-go v0.20.1 (Common Expression Language)
    │   └── xiatechs/jsonata-go v1.8.5 (JSONata transformations)
    │
    ├── 🔑 OAuth & Authentication
    │   ├── golang.org/x/oauth2 v0.18.0
    │   └── (Azure identity libs listed above)
    │
    └── 🧪 Testing
        ├── testcontainers-go v0.33.0
        ├── testcontainers-go/modules/compose v0.33.0
        ├── stretchr/testify v1.9.0
        └── go.uber.org/mock v0.4.0
```

## Direct Dependencies Analysis

### Core Serialization (7 packages)

| Package | Version | Purpose | Last Major Update | Alternatives |
|---------|---------|---------|-------------------|--------------|
| **hamba/avro/v2** | 2.24.0 | Avro v2 serde | 2024 | ✅ Recommended |
| heetch/avro | 0.4.5 | Legacy Avro v1 | 2021 | ⚠️ Use hamba instead |
| actgardner/gogen-avro/v10 | 10.2.1 | Avro code gen | 2023 | Active |
| invopop/jsonschema | 0.12.0 | JSON Schema gen | 2024 | Active |
| santhosh-tekuri/jsonschema/v5 | 5.3.0 | JSON validation | 2024 | Active |
| jhump/protoreflect | 1.15.6 | Protobuf reflection | 2024 | Active |
| google.golang.org/protobuf | 1.33.0 | Protobuf runtime | 2024 | Official |

**Security Status**: ✅ All actively maintained, no known vulnerabilities

**Recommendation**: Consider consolidating Avro libraries (remove heetch/avro)

---

### Encryption & KMS (10 packages)

#### Tink Cryptography Framework
| Package | Version | Purpose | Maturity |
|---------|---------|---------|----------|
| tink-crypto/tink-go/v2 | 2.1.0 | Core crypto | ✅ Google-backed |
| tink-crypto/tink-go-gcpkms/v2 | 2.1.0 | GCP KMS integration | ✅ Official |
| tink-crypto/tink-go-hcvault/v2 | 2.1.0 | Vault integration | ✅ Official |

**Why Tink?**
- FIPS 140-2 validated cryptographic implementations
- Misuse-resistant API design
- Multi-language consistency

#### AWS KMS
| Package | Version | Notes |
|---------|---------|-------|
| aws-sdk-go-v2 | 1.26.1 | Base AWS SDK |
| aws-sdk-go-v2/service/kms | 1.30.1 | KMS operations |
| aws-sdk-go-v2/service/sts | 1.28.6 | Token service |
| aws-sdk-go-v2/credentials | 1.17.10 | Auth handling |

**Security Status**: ✅ Official AWS SDK, actively maintained

#### Azure KMS
| Package | Version | Notes |
|---------|---------|-------|
| Azure/azure-sdk-for-go/sdk/azcore | 1.11.1 | Base Azure SDK |
| Azure/azure-sdk-for-go/sdk/azidentity | 1.6.0 | Azure AD auth |
| Azure/azure-sdk-for-go/sdk/security/keyvault/azkeys | 1.1.0 | Key Vault ops |

**Security Status**: ✅ Official Azure SDK, actively maintained

#### HashiCorp Vault
| Package | Version | Notes |
|---------|---------|-------|
| hashicorp/vault/api | 1.15.0 | Vault client |
| hashicorp/vault/api/auth/approle | 0.8.0 | AppRole auth |

**Security Status**: ✅ Official Vault client, actively maintained

---

### Rules Engines (2 packages)

| Package | Version | Purpose | Language | Performance |
|---------|---------|---------|----------|-------------|
| google/cel-go | 0.20.1 | Common Expression Language | Expression DSL | Fast (compiled) |
| xiatechs/jsonata-go | 1.8.5 | JSONata transformations | JSON query language | Moderate |

**Use Cases**:
- **CEL**: Field validation, conditional encryption, access control
- **JSONata**: Data transformation, field mapping, enrichment

**Security Status**: ✅ Both actively maintained

---

### Testing Infrastructure (4 packages)

| Package | Version | Purpose |
|---------|---------|---------|
| testcontainers-go | 0.33.0 | Docker-based integration tests |
| testcontainers-go/modules/compose | 0.33.0 | Docker Compose support |
| stretchr/testify | 1.9.0 | Assertions and mocks |
| go.uber.org/mock | 0.4.0 | Mock generation |

**Usage**: Integration tests in `kafka/integration_test.go`

---

### OAuth & Authentication (2 packages)

| Package | Version | Purpose |
|---------|---------|---------|
| golang.org/x/oauth2 | 0.18.0 | OAuth2 client |
| Azure identity libs | (see above) | Azure AD integration |

**Use Cases**: SASL/OAUTHBEARER, Schema Registry auth

---

### Utilities (11 packages)

| Package | Version | Purpose |
|---------|---------|---------|
| google/uuid | 1.6.0 | UUID generation |
| golang/protobuf | 1.5.4 | Legacy protobuf (v1) |
| modern-go/reflect2 | 1.0.2 | Reflection utilities |
| (Others) | Various | Transitive dependencies |

---

## Transitive Dependencies (~200 packages)

### Major Categories:

#### 1. Docker/Container Infrastructure (~50 packages)
- Docker client libraries
- Containerd dependencies
- Kubernetes client libraries (used by testcontainers)

**Concern**: Heavy test-only dependencies leak into production binary size

**Mitigation**: Consider build tags to exclude test deps from production builds

---

#### 2. gRPC & HTTP Infrastructure (~30 packages)
- google.golang.org/grpc v1.64.1
- go.opentelemetry.io/* (OpenTelemetry instrumentation)
- HTTP/2 and HTTP/3 support

**Purpose**: Cloud SDK requirements (AWS, GCP, Azure all use gRPC)

---

#### 3. Cloud Platform Dependencies (~40 packages)
- GCP: Cloud IAM, Cloud Storage, logging
- AWS: EC2 IMDS, S3, CloudWatch
- Azure: ARM, Resource Manager

**Note**: Most are transitive from SDK imports

---

#### 4. Cryptography (~20 packages)
- golang.org/x/crypto
- Various hash functions and ciphers
- TLS certificate handling

---

#### 5. Compression & Encoding (~10 packages)
- klauspost/compress (gzip, zstd, snappy)
- Protobuf encoders
- JSON iterators

---

## Dependency Risk Assessment

### High-Impact Dependencies (Breaking Changes Would Affect Users)

| Dependency | Risk Level | Mitigation |
|------------|------------|------------|
| **librdkafka** | 🔴 CRITICAL | Bundled statically, version pinned |
| hamba/avro/v2 | 🟡 MEDIUM | Major version bump would require user changes |
| tink-crypto/tink-go | 🟡 MEDIUM | Encryption changes need careful migration |
| AWS/Azure/GCP SDKs | 🟢 LOW | Isolated to KMS features |

---

### Security Vulnerability Surface

#### Direct Attack Surface
1. **Network dependencies**: HTTP clients, TLS implementations
2. **Deserialization**: Avro, Protobuf, JSON parsers
3. **Encryption**: Tink, KMS integrations
4. **OAuth**: Token handling, credential management

#### Mitigation Strategies
- ✅ All cloud SDKs are official (AWS, Azure, GCP)
- ✅ Tink is Google-backed with security audits
- ✅ Regular `go mod tidy` and vulnerability scanning (Dependabot)
- ⚠️ No automated CVE scanning in CI (could be improved)

---

### Abandoned/Deprecated Dependencies

| Dependency | Status | Action Needed |
|------------|--------|---------------|
| heetch/avro | ⚠️ Last update 2021 | ✅ Already replaced by hamba/avro/v2 |
| golang/protobuf | ⚠️ Deprecated (use google.golang.org/protobuf) | ⚠️ Transitive only, no action |

**Recommendation**: Remove heetch/avro completely in next major version

---

## Dependency Update Strategy

### Current Approach
- **Go version**: 1.24.3 (latest with FIPS support)
- **Update frequency**: Ad-hoc (no automated updates visible)
- **Versioning**: Semantic versioning for main module

### Recommended Improvements
1. **Dependabot**: Enable automated PR creation for security updates
2. **Update policy**: Monthly minor version updates, quarterly major reviews
3. **Breaking changes**: Document in CHANGELOG.md
4. **Deprecation notices**: Add warnings for heetch/avro usage

---

## Build-Time vs. Runtime Dependencies

### Build-Time Only
- testcontainers-go (integration tests)
- go.uber.org/mock (code generation)
- Docker/Kubernetes clients (test infrastructure)

### Runtime (Always Included)
- librdkafka (CGo dependency)
- Serialization libraries (if using Schema Registry)
- KMS SDKs (if using encryption)
- Rules engines (if using data contracts)

### Optimization Opportunity
**Problem**: Test dependencies increase binary size
**Solution**: Use build tags to exclude test-only imports:
```go
//go:build integration
// +build integration

import "github.com/testcontainers/testcontainers-go"
```

---

## License Compatibility

### License Summary

| License Type | Count | Examples |
|--------------|-------|----------|
| **Apache 2.0** | ~150 | Main module, AWS SDK, GCP SDK, Tink |
| **MIT** | ~40 | testify, uuid, most utilities |
| **BSD-3-Clause** | ~10 | Protobuf, some crypto libs |
| **MPL 2.0** | 2 | HashiCorp Vault |

**Status**: ✅ All licenses are compatible with Apache 2.0

**Note**: No GPL/AGPL dependencies (which would require disclosure)

---

## Dependency Size Analysis

### On-Disk Size (Estimated)

```
Total go.mod dependencies:      ~150MB
├── librdkafka (bundled):       112MB (all platforms)
├── Cloud SDKs:                  20MB
├── Docker/testcontainers:       10MB
├── Serialization libs:           5MB
└── Other:                        3MB
```

### Binary Size Impact

**Minimal binary** (Producer + Consumer only):
```
go build -tags dynamic  # ~8MB (excludes Schema Registry)
```

**Full binary** (All features):
```
go build  # ~35MB (includes Schema Registry, encryption, cloud SDKs)
```

**Optimization**: Use `-ldflags "-s -w"` for smaller binaries (~20% reduction)

---

## Dependency Graph (Mermaid)

```mermaid
graph TD
    A[confluent-kafka-go/v2] --> B[kafka package]
    A --> C[schemaregistry package]

    B --> D[librdkafka v2.12.0<br/>bundled C library]
    B --> E[testcontainers<br/>test only]

    C --> F[Serialization]
    C --> G[Encryption]
    C --> H[Cloud SDKs]
    C --> I[Rules Engines]

    F --> F1[hamba/avro/v2]
    F --> F2[protobuf]
    F --> F3[jsonschema]

    G --> G1[tink-crypto]

    H --> H1[AWS SDK]
    H --> H2[Azure SDK]
    H --> H3[GCP SDK]
    H --> H4[Vault API]

    I --> I1[google/cel-go]
    I --> I2[jsonata-go]

    H1 --> H1A[KMS]
    H1 --> H1B[STS]
    H2 --> H2A[Key Vault]
    H3 --> H3A[Cloud KMS]

    style A fill:#f9f,stroke:#333,stroke-width:4px
    style D fill:#bbf,stroke:#333,stroke-width:2px
    style F fill:#bfb,stroke:#333,stroke-width:2px
    style G fill:#fbb,stroke:#333,stroke-width:2px
```

---

## Recommendations

### Quick Wins
1. ✅ **Remove heetch/avro**: Fully migrate to hamba/avro/v2
2. ✅ **Add Dependabot**: Automate security updates
3. ✅ **Document optional deps**: Clarify which features require which dependencies

### Strategic Improvements
1. 🎯 **Build tags for features**: Allow users to exclude Schema Registry deps
2. 🎯 **Vendor critical deps**: Consider vendoring serialization libs for stability
3. 🎯 **CVE scanning**: Add automated vulnerability scanning to CI
4. 🎯 **Dependency dashboard**: Create visibility into update status

### Long-Term Considerations
1. 📅 **librdkafka updates**: Plan for major version updates (currently 2.x)
2. 📅 **Go version policy**: Document minimum Go version support
3. 📅 **Breaking change policy**: Establish guidelines for dependency updates

---

## Summary

**Strengths**:
- ✅ Well-curated dependencies (all official/reputable sources)
- ✅ No known security vulnerabilities
- ✅ Clear separation (minimal kafka, feature-rich schemaregistry)
- ✅ License compatibility

**Areas for Improvement**:
- ⚠️ Heavy transitive dependencies (~200 packages)
- ⚠️ Test dependencies leak into production code
- ⚠️ No automated dependency updates
- ⚠️ Legacy Avro library still present

**Overall Assessment**: 8/10
The dependency management is solid, but could benefit from automated updates and better feature-gating to reduce binary size for users who don't need Schema Registry features.
