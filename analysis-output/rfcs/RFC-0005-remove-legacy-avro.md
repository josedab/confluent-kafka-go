# RFC-0005: Remove Legacy Avro (heetch/avro) Dependency

**Status:** Draft 📝
**Author:** Technical Analysis Team
**Created:** 2025-11-16
**Priority:** P1 (Technical Debt)

## Summary

Remove the deprecated `heetch/avro` dependency in favor of `hamba/avro/v2`, reducing maintenance burden and dependency count. This is a breaking change for v3.0.

## Motivation

**Current State:** Two Avro implementations coexist
- `schemaregistry/serde/avro/` (heetch/avro) - Legacy
- `schemaregistry/serde/avrov2/` (hamba/avro/v2) - Modern, recommended

**Problems:**
- Confusing for users (which to use?)
- Double maintenance (bugs, updates)
- heetch/avro last updated 2021 (low activity)

## Detailed Design

### Phase 1: Deprecation (v2.x)

Add deprecation warnings:

```go
// Package avro provides Avro serialization using heetch/avro.
//
// Deprecated: Use schemaregistry/serde/avrov2 instead.
// This package will be removed in v3.0.
package avro
```

### Phase 2: Migration Guide

Provide automatic migration tool:

```bash
# Migration script
go run github.com/confluentinc/confluent-kafka-go/v2/tools/migrate-avro
```

**Example migration:**

**Before (heetch/avro):**
```go
import "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"

serializer, _ := avro.NewSerializer(client, serde.ValueSerde, avro.NewSerializerConfig())
```

**After (hamba/avro/v2):**
```go
import "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avrov2"

serializer, _ := avrov2.NewSerializer(client, serde.ValueSerde, avrov2.NewSerializerConfig())
```

### Phase 3: Removal (v3.0)

Delete `schemaregistry/serde/avro/` directory entirely.

## Implementation Plan

**v2.x (Current):**
- Week 1: Add deprecation warnings
- Week 1: Create migration guide
- Week 1: Update all examples to use avrov2

**v2.x+1 (Next release):**
- Louder deprecation warnings (print to stderr on first use)

**v3.0 (Major version):**
- Remove heetch/avro completely
- Remove dependency from go.mod

## Backward Compatibility

**Breaking Change** (v3.0 only)
- Users must migrate to avrov2 before upgrading to v3.0
- 6-month deprecation period (v2.x releases)

## Success Criteria

- ✅ All examples use avrov2
- ✅ Migration guide tested on real projects
- ✅ Zero new issues using deprecated avro package

**Effort:** 1 week
**Impact:** Medium (cleanup, breaking change in v3.0)
