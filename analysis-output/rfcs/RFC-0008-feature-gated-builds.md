# RFC-0008: Feature-Gated Builds (Reduce Binary Size)

**Status:** Draft 📝
**Author:** Technical Analysis Team
**Created:** 2025-11-16
**Priority:** P2 (Optimization)

## Summary

Implement build tags to allow users to exclude Schema Registry and other optional features, reducing binary size from ~35MB to ~8MB for users who only need basic Kafka Producer/Consumer functionality.

## Motivation

**Current Binary Sizes:**
- Full build: ~35MB (includes Schema Registry, all KMS providers, rules engines)
- Kafka only: ~8MB (Producer/Consumer/Admin only)

**Problem:** Many users don't need Schema Registry but pay the binary size cost

## Detailed Design

### Build Tags

```go
// schemaregistry/schemaregistry_client.go
//go:build !noschemaregistry

package schemaregistry
// ... implementation
```

```go
// schemaregistry/schemaregistry_stub.go (new file)
//go:build noschemaregistry

package schemaregistry

// Stub implementation that panics if used
func NewClient(config *Config) (Client, error) {
    panic("Schema Registry support not compiled in. Rebuild without -tags noschemaregistry")
}
```

### Build Configurations

**Default (full features):**
```bash
go build ./...
# Size: ~35MB
```

**Minimal (Kafka only):**
```bash
go build -tags minimal ./...
# Size: ~8MB
# Excludes: Schema Registry, encryption, rules
```

**No Schema Registry:**
```bash
go build -tags noschemaregistry ./...
# Size: ~12MB
# Excludes: Schema Registry only
```

## Implementation Plan

**Week 1-2: Tag Infrastructure**
- Add build tags to schemaregistry package
- Create stub implementations
- Update Makefile with build targets

**Week 3: Testing**
- CI must test all build configurations
- Verify binary sizes
- Test stub panics

**Week 4: Documentation**
- Update README with build options
- Add to architecture docs

## Backward Compatibility

✅ **100% Backward Compatible** (default build unchanged)

## Success Criteria

- ✅ Minimal build < 10MB
- ✅ All build configurations tested in CI
- ✅ Clear error messages if feature not compiled

**Effort:** 3 weeks
**Impact:** Medium (size optimization)
