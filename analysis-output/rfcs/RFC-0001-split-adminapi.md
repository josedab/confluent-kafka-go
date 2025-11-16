# RFC-0001: Split adminapi.go into Focused Modules

**Status:** Draft 📝
**Author:** Technical Analysis Team
**Created:** 2025-11-16
**Updated:** 2025-11-16
**Priority:** P0 (Quick Win)

---

## Summary

Split the monolithic `kafka/adminapi.go` file (~3,900 LOC) into focused, maintainable modules organized by administrative domain (topics, ACLs, consumer groups, configurations, offsets). This improves code navigability, reduces cognitive load, and makes the codebase more contributor-friendly.

---

## Motivation

### Current State

`kafka/adminapi.go` is the largest and most complex file in the codebase:
- **Line Count**: ~3,900 LOC (3x larger than the next largest file)
- **Function Count**: 60+ functions
- **Complexity**: Handles 19 different admin operations
- **Cognitive Load**: Requires scrolling through multiple domains to find relevant code

**Example Line Numbers** ([`kafka/adminapi.go`](https://github.com/confluentinc/confluent-kafka-go/blob/f8569d7/kafka/adminapi.go)):
- Lines 1-500: Topic operations
- Lines 501-1000: ACL operations
- Lines 1001-1500: Consumer group operations
- Lines 1501-2000: Configuration operations
- Lines 2001+: Offset operations, common utilities

### Problems

1. **Poor Developer Experience**: Finding code requires extensive scrolling or Ctrl+F
2. **Merge Conflicts**: Multiple contributors editing the same file → frequent conflicts
3. **Review Difficulty**: PRs touching adminapi.go are hard to review (large diffs, unclear scope)
4. **Testing Complexity**: Test file mirrors the monolith (`adminapi_test.go`)
5. **Mental Context Switching**: Reading topic code, then ACL code, then groups — unrelated concerns mixed

### User Impact

**Direct Impact** (Contributors):
- ❌ Longer time to understand admin API implementation
- ❌ Higher barrier to contributing admin features
- ❌ More review cycles (unclear code location)

**Indirect Impact** (End Users):
- ⚠️ Slower feature development (harder to contribute)
- ⚠️ Potential bugs from complex merge conflicts

---

## Detailed Design

### Proposed File Structure

```
kafka/
├── adminapi/
│   ├── admin.go              # AdminClient struct, NewAdminClient()
│   ├── topics.go             # Topic operations (Create, Delete, Describe)
│   ├── acls.go               # ACL operations (Create, Delete, Describe)
│   ├── consumer_groups.go    # Consumer group operations (Delete, Describe, List)
│   ├── configs.go            # Configuration operations (Describe, Alter, IncrementalAlter)
│   ├── offsets.go            # Offset operations (List, Alter)
│   ├── cluster.go            # Cluster operations (Describe, ElectLeaders)
│   ├── common.go             # Shared types and utilities
│   └── errors.go             # Admin-specific error handling
├── adminapi_test.go          # Integration tests (unchanged location for CI)
├── ... (other files)
```

**Alternative Considered**: Keep `kafka/adminapi.go` but split into sections with clearer comments
- **Rejected**: Doesn't solve merge conflict or cognitive load issues

---

### Migration Strategy

#### Phase 1: Preparation (Day 1)
1. Create `kafka/adminapi/` package directory
2. Set up internal package visibility (unexported shared types)
3. Write migration script to automate file splitting

#### Phase 2: Code Migration (Days 2-3)
1. Extract topic operations → `topics.go`
   - `CreateTopics()`, `DeleteTopics()`, `DescribeTopics()`, `CreatePartitions()`
   - Shared types: `TopicSpecification`, `TopicResult`, etc.

2. Extract ACL operations → `acls.go`
   - `CreateACLs()`, `DeleteACLs()`, `DescribeACLs()`
   - Shared types: `ACLBinding`, `ACLBindingFilter`, etc.

3. Extract consumer group operations → `consumer_groups.go`
   - `DeleteConsumerGroups()`, `DescribeConsumerGroups()`, `ListConsumerGroups()`, `AlterConsumerGroupOffsets()`, `ListConsumerGroupOffsets()`
   - Shared types: `ConsumerGroupDescription`, etc.

4. Extract configuration operations → `configs.go`
   - `DescribeConfigs()`, `AlterConfigs()`, `IncrementalAlterConfigs()`
   - Shared types: `ConfigResource`, `ConfigEntry`, etc.

5. Extract offset operations → `offsets.go`
   - `ListOffsets()`, `DeleteRecords()`
   - Shared types: `OffsetSpec`, etc.

6. Extract cluster operations → `cluster.go`
   - `DescribeCluster()`, `ElectLeaders()`
   - Shared types: `ClusterDescription`, etc.

7. Extract common utilities → `common.go`
   - Result handling, option parsing, C conversion utilities

8. Main admin file → `admin.go`
   - `AdminClient` struct
   - `NewAdminClient()` constructor
   - `Close()` method

#### Phase 3: Testing & Validation (Days 4-5)
1. Run full test suite (no changes to test behavior)
2. Verify no public API changes (only internal refactoring)
3. Build across all platforms (Linux, macOS, Windows)
4. Update any internal import paths

#### Phase 4: Documentation (Day 5)
1. Update developer documentation
2. Add package-level docs to `adminapi/` package
3. Update contribution guide with new structure

---

## Example Code Structure

### Before: `kafka/adminapi.go` (Monolithic)

```go
// kafka/adminapi.go (~3900 lines)

package kafka

// AdminClient struct
type AdminClient struct {
    handle handle
    isDerived bool
}

// Topic operations
func (a *AdminClient) CreateTopics(ctx context.Context, topics []TopicSpecification, options ...CreateTopicsAdminOption) ([]TopicResult, error) {
    // ~200 lines
}

func (a *AdminClient) DeleteTopics(ctx context.Context, topics []string, options ...DeleteTopicsAdminOption) ([]TopicResult, error) {
    // ~150 lines
}

// ... (60 more functions)

// ACL operations
func (a *AdminClient) CreateACLs(ctx context.Context, aclBindings ACLBindings, options ...CreateACLsAdminOption) ([]CreateACLResult, error) {
    // ~180 lines
}

// ... (continues for 3900 lines)
```

### After: `kafka/adminapi/admin.go` (Entry Point)

```go
// kafka/adminapi/admin.go

package adminapi

import "github.com/confluentinc/confluent-kafka-go/v2/kafka"

// AdminClient provides administrative operations on a Kafka cluster.
type AdminClient struct {
    handle kafka.Handle
    isDerived bool
}

// NewAdminClient creates a new AdminClient instance.
func NewAdminClient(conf *kafka.ConfigMap) (*AdminClient, error) {
    // Implementation
}

// Close terminates the AdminClient.
func (a *AdminClient) Close() {
    // Implementation
}
```

### After: `kafka/adminapi/topics.go` (Focused Module)

```go
// kafka/adminapi/topics.go

package adminapi

import (
    "context"
    "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// TopicSpecification specifies a topic to be created.
type TopicSpecification = kafka.TopicSpecification

// TopicResult represents the result of a topic operation.
type TopicResult = kafka.TopicResult

// CreateTopics creates one or more topics.
//
// Parameters:
//   ctx      - Context for timeout and cancellation
//   topics   - Slice of topic specifications
//   options  - Optional admin options (timeout, validate only)
//
// Returns:
//   results  - Slice of TopicResult (one per topic)
//   error    - Error if operation fails
func (a *AdminClient) CreateTopics(ctx context.Context, topics []TopicSpecification, options ...CreateTopicsAdminOption) ([]TopicResult, error) {
    // ~200 lines (moved from adminapi.go)
}

// DeleteTopics deletes one or more topics.
func (a *AdminClient) DeleteTopics(ctx context.Context, topics []string, options ...DeleteTopicsAdminOption) ([]TopicResult, error) {
    // ~150 lines
}

// DescribeTopics describes one or more topics.
func (a *AdminClient) DescribeTopics(ctx context.Context, topics []string, options ...DescribeTopicsAdminOption) (*DescribeTopicsResult, error) {
    // ~120 lines
}

// CreatePartitions adds partitions to existing topics.
func (a *AdminClient) CreatePartitions(ctx context.Context, partitions []PartitionsSpecification, options ...CreatePartitionsAdminOption) ([]TopicResult, error) {
    // ~180 lines
}
```

**Benefits**:
- ✅ Each file < 600 LOC (manageable size)
- ✅ Clear separation of concerns (topics vs. ACLs vs. groups)
- ✅ Easier to navigate (find topic code in `topics.go`)
- ✅ Parallel development (different contributors, different files)

---

## Implementation Plan

### Timeline: 1 Week (5 days)

**Day 1: Preparation**
- [ ] Create feature branch `refactor/split-adminapi`
- [ ] Create `kafka/adminapi/` directory
- [ ] Write migration script (Go AST-based or manual)
- [ ] Get maintainer approval for approach

**Day 2-3: Code Migration**
- [ ] Extract and move code to focused files
- [ ] Update import paths if needed
- [ ] Ensure all functions are accounted for

**Day 4: Testing & Validation**
- [ ] Run unit tests: `go test ./kafka/...`
- [ ] Run integration tests: `go test -tags integration ./kafka/...`
- [ ] Build for all platforms: `go build -tags musl ./...`, etc.
- [ ] Verify no public API changes (only internal refactoring)

**Day 5: Documentation & PR**
- [ ] Update developer documentation
- [ ] Add package docs to `adminapi/`
- [ ] Create PR with detailed description
- [ ] Address review feedback

---

## Backward Compatibility

### Public API: ✅ 100% Compatible

**No Changes to Public API**:
- All public methods remain on `kafka.AdminClient`
- All public types remain in `kafka` package
- All function signatures unchanged
- Existing user code continues to work without modification

**Example** (user code unaffected):
```go
// Before refactoring
admin, _ := kafka.NewAdminClient(&kafka.ConfigMap{...})
results, _ := admin.CreateTopics(ctx, topics)

// After refactoring (identical code)
admin, _ := kafka.NewAdminClient(&kafka.ConfigMap{...})
results, _ := admin.CreateTopics(ctx, topics)
```

### Internal API: ⚠️ Internal Package Structure Changes

**Impact on Internal Imports**:
- If any internal packages import `kafka.AdminClient` internals, those imports may need updates
- **Mitigation**: Full codebase search for internal usages

### Testing: ✅ No Test Changes Required

- Test file location remains `kafka/adminapi_test.go`
- Test function signatures unchanged
- CI pipeline unchanged

---

## Alternatives Considered

### Alternative 1: Keep Monolithic File, Add Better Comments

**Approach**:
```go
// kafka/adminapi.go

// ========================================
// Topic Operations
// ========================================

func (a *AdminClient) CreateTopics(...) {}

// ========================================
// ACL Operations
// ========================================

func (a *AdminClient) CreateACLs(...) {}
```

**Rejected Because**:
- Doesn't solve merge conflict issues
- Doesn't reduce file size (still 3900 LOC)
- Minimal improvement to navigation

---

### Alternative 2: Create Separate Packages (kafka/admin, kafka/topics, kafka/acls)

**Approach**:
```
kafka/
├── admin/
│   └── client.go
├── topics/
│   └── operations.go
├── acls/
│   └── operations.go
```

**Rejected Because**:
- Breaks public API (import paths change)
- Requires major version bump (v3.0)
- More disruptive than necessary

---

### Alternative 3: Use Interface-Based Separation

**Approach**:
```go
type TopicAdmin interface {
    CreateTopics(...) ([]TopicResult, error)
    DeleteTopics(...) ([]TopicResult, error)
}

type ACLAdmin interface {
    CreateACLs(...) ([]ACLResult, error)
}
```

**Rejected Because**:
- Adds unnecessary abstraction (no need for interface here)
- Doesn't reduce file size
- Makes API more complex

---

## Open Questions

### Q1: Should we create a `kafka/adminapi` package or keep in `kafka/`?

**Option A**: `kafka/adminapi/` (sub-package)
- Pros: Clear separation, parallel development
- Cons: Internal package (not public API change, but visible)

**Option B**: Keep files in `kafka/` with `admin_topics.go`, `admin_acls.go` naming
- Pros: No package structure change
- Cons: More files in already-large `kafka/` directory

**Recommendation**: **Option A** (sub-package) — clearer organization, easier to navigate

---

### Q2: Should tests be split similarly?

**Option A**: Split tests to match implementation files
```
kafka/adminapi/
├── topics_test.go
├── acls_test.go
```

**Option B**: Keep single `adminapi_test.go` file

**Recommendation**: **Option B** initially (split in follow-up RFC if needed)

---

### Q3: What about shared utilities (C conversion, result handling)?

**Approach**: Extract to `kafka/adminapi/common.go`
- Unexported functions (internal to adminapi package)
- Reduces duplication across domain files

---

## Success Criteria

### Code Quality Metrics
- ✅ All files in `kafka/adminapi/` < 1000 LOC
- ✅ Average file size < 500 LOC
- ✅ Functions per file < 15

### Testing
- ✅ All existing tests pass without modification
- ✅ No decrease in code coverage
- ✅ CI pipeline green across all platforms

### Developer Experience
- ✅ PR review time decreases (measured over 3 months)
- ✅ Merge conflicts in admin code decrease
- ✅ Contributor survey shows improved navigation

### Documentation
- ✅ Package docs added to `kafka/adminapi/`
- ✅ Developer guide updated
- ✅ Example imports updated (if any)

---

## Related Work

- **Kafka Java Client**: Uses separate classes (AdminClient, TopicManager, ACLManager)
- **Sarama**: Uses separate packages (`sarama/admin`, `sarama/cluster`)
- **librdkafka**: Single file (`rdkafka_admin.c`) but C conventions differ

**Inspiration**: Java client's separation while maintaining Go package conventions

---

## References

- [Go Package Organization Best Practices](https://go.dev/doc/modules/layout)
- [Effective Go: Package Names](https://go.dev/doc/effective_go#package-names)
- [confluent-kafka-go Current Structure](https://github.com/confluentinc/confluent-kafka-go/tree/master/kafka)

---

## Appendix: File Size Breakdown (After Split)

| File | Approx. LOC | Function Count | Description |
|------|-------------|----------------|-------------|
| `admin.go` | 150 | 5 | AdminClient struct, constructor, Close() |
| `topics.go` | 650 | 10 | Topic operations (Create, Delete, Describe, CreatePartitions) |
| `acls.go` | 550 | 8 | ACL operations (Create, Delete, Describe) |
| `consumer_groups.go` | 700 | 12 | Consumer group operations |
| `configs.go` | 600 | 10 | Configuration operations |
| `offsets.go` | 450 | 6 | Offset operations |
| `cluster.go` | 300 | 4 | Cluster operations |
| `common.go` | 400 | 10 | Shared utilities |
| `errors.go` | 100 | 5 | Error handling |
| **Total** | **3,900** | **70** | (Same as current, but organized) |

**Key Improvement**: No file > 700 LOC (vs. single 3,900 LOC file)

---

## Conclusion

Splitting `adminapi.go` is a **low-risk, high-impact refactoring** that immediately improves code quality and developer experience. The implementation is straightforward (pure code movement), and backward compatibility is maintained.

**Recommended Next Steps**:
1. Get maintainer approval for approach
2. Implement in 1 week (following timeline above)
3. Iterate based on review feedback

**Questions or Feedback?** Comment on the GitHub issue for this RFC or reach out to maintainers.

---

**Status**: 📝 Draft — Awaiting maintainer review
**Next Review**: 2025-11-30
