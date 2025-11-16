# RFC Prioritization Matrix

**Date:** 2025-11-16
**Based on Commit:** `f8569d7adaae6575fff184a0b3e5c0cab025e19f`

## Overview

This document prioritizes 10 improvement proposals for confluent-kafka-go based on **impact** and **effort**. Each RFC addresses specific gaps or opportunities identified during codebase analysis.

## Prioritization Framework

### Impact Scale (1-5)
- **5**: Critical to user success, affects all users
- **4**: Significant improvement, affects most users
- **3**: Notable improvement, affects some users
- **2**: Minor improvement, niche use case
- **1**: Nice-to-have, minimal user impact

### Effort Scale (1-5)
- **5**: >1 month, requires architecture changes
- **4**: 2-4 weeks, significant implementation
- **3**: 1-2 weeks, moderate implementation
- **2**: 3-5 days, straightforward changes
- **1**: 1-2 days, quick wins

### Priority Categories
- **Quick Wins**: High impact, low effort (do first)
- **Strategic**: High impact, high effort (plan carefully)
- **Fill-ins**: Low impact, low effort (when time permits)
- **Reconsider**: Low impact, high effort (probably skip)

---

## RFC Matrix

| RFC | Title | Impact | Effort | Priority | Category |
|-----|-------|--------|--------|----------|----------|
| **RFC-0001** | Split adminapi.go into Focused Modules | 4 | 2 | **P0** | Quick Win |
| **RFC-0002** | Add Structured Logging with slog | 5 | 3 | **P0** | Strategic |
| **RFC-0003** | Implement Metrics Exporter (Prometheus) | 4 | 3 | **P1** | Strategic |
| **RFC-0004** | Add Code Coverage Reporting to CI | 3 | 1 | **P0** | Quick Win |
| **RFC-0005** | Remove Legacy Avro (heetch/avro) Dependency | 3 | 2 | **P1** | Quick Win |
| **RFC-0006** | Add Object Pooling for High-Throughput Scenarios | 4 | 4 | **P2** | Strategic |
| **RFC-0007** | Create Architecture Documentation Site | 5 | 3 | **P1** | Strategic |
| **RFC-0008** | Feature-Gated Builds (Reduce Binary Size) | 3 | 4 | **P2** | Strategic |
| **RFC-0009** | Enhanced Error Messages with Context | 4 | 2 | **P1** | Quick Win |
| **RFC-0010** | Add OpenTelemetry Integration | 4 | 4 | **P2** | Strategic |

---

## Visualization

```
Impact vs. Effort Matrix

 5│
  │         RFC-0007
  │         (Arch Docs)
I 4│  RFC-0001           RFC-0002     RFC-0003  RFC-0006
m  │  (Split Admin)      (Logging)    (Metrics) (Pooling)
p 3│  RFC-0004  RFC-0005                         RFC-0008
a  │  (Coverage)(Avro)                            (Feature Gates)
c 2│         RFC-0009                             RFC-0010
t  │         (Errors)                             (OTel)
 1│
  │
  └─────────────────────────────────────────────
    1        2         3          4         5
                    Effort (dev-weeks)

Legend:
  ● Quick Wins (High Impact, Low Effort)
  ○ Strategic (High Impact, High Effort)
  △ Fill-ins (Low Impact, Low Effort)
  ▽ Reconsider (Low Impact, High Effort)
```

---

## Quick Wins (Do First)

### RFC-0001: Split adminapi.go into Focused Modules
**Impact:** 4/5 | **Effort:** 2/5 | **Timeline:** 1 week

**Why Quick Win:**
- Clear immediate benefit (better code organization)
- Low risk (pure refactoring)
- Improves contributor experience significantly

**Execution Plan:**
1. Week 1: Split file into logical modules (topics, ACLs, groups, configs)
2. Week 1: Update tests, verify no behavior changes
3. Week 1: Update documentation

**Success Metrics:**
- ✅ No file >1000 LOC in kafka package
- ✅ All tests pass without modification
- ✅ Contributor PR review time decreases

---

### RFC-0004: Add Code Coverage Reporting to CI
**Impact:** 3/5 | **Effort:** 1/5 | **Timeline:** 2 days

**Why Quick Win:**
- Minimal implementation (GitHub Actions + CodeCov)
- High visibility (badges, dashboards)
- Foundation for future quality improvements

**Execution Plan:**
- Day 1: Add coverage collection to CI workflow
- Day 1: Configure CodeCov integration
- Day 2: Add coverage badge to README, set coverage targets

**Success Metrics:**
- ✅ Coverage visible in every PR
- ✅ Coverage targets enforced (e.g., no decrease)
- ✅ Coverage badge in README

---

### RFC-0009: Enhanced Error Messages with Context
**Impact:** 4/5 | **Effort:** 2/5 | **Timeline:** 1 week

**Why Quick Win:**
- Immediately improves user experience
- Incremental implementation (can be done per error type)
- Low risk (backward compatible)

**Execution Plan:**
1. Week 1: Audit top 20 error messages
2. Week 1: Add context (configuration hints, troubleshooting steps)
3. Week 1: Update error generation code

**Success Metrics:**
- ✅ Top 20 errors have actionable guidance
- ✅ Support questions decrease (track via GitHub issues)

---

## Strategic Initiatives (High Impact, Plan Carefully)

### RFC-0002: Add Structured Logging with slog
**Impact:** 5/5 | **Effort:** 3/5 | **Timeline:** 2 weeks

**Why Strategic:**
- Affects all users (logging is universal)
- Requires careful migration (backward compatibility)
- Enables future observability improvements

**Phased Approach:**
1. Phase 1 (Week 1): Add slog alongside existing logging
2. Phase 2 (Week 2): Migrate internal logging to slog
3. Phase 3 (Future): Deprecate old logging API

**Dependencies:**
- Go 1.21+ (slog introduced in 1.21)
- User migration guide

---

### RFC-0003: Implement Metrics Exporter (Prometheus)
**Impact:** 4/5 | **Effort:** 3/5 | **Timeline:** 2 weeks

**Why Strategic:**
- Critical for production observability
- Requires design (metrics selection, labels)
- Integration with existing stats events

**Phased Approach:**
1. Phase 1 (Week 1): Define metrics schema (producer/consumer metrics)
2. Phase 2 (Week 2): Implement Prometheus exporter
3. Phase 3 (Future): Add Grafana dashboard examples

**Key Metrics to Expose:**
- Producer: messages sent, bytes sent, errors, latency percentiles
- Consumer: messages consumed, bytes consumed, lag, rebalances

---

### RFC-0007: Create Architecture Documentation Site
**Impact:** 5/5 | **Effort:** 3/5 | **Timeline:** 2 weeks

**Why Strategic:**
- Critical for contributor onboarding
- Reduces support burden
- Foundation for community growth

**Content Outline:**
1. Architecture overview (this blog series!)
2. CGo integration guide
3. Build system documentation
4. Contribution guidelines
5. Performance tuning guide

**Tooling:** Docusaurus, MkDocs, or similar

---

## Medium Priority (Strategic, Longer Timeline)

### RFC-0006: Add Object Pooling for High-Throughput Scenarios
**Impact:** 4/5 | **Effort:** 4/5 | **Timeline:** 3 weeks

**Why Medium Priority:**
- Significant performance benefit (30-50% allocation reduction)
- Requires careful design (pool sizing, lifecycle)
- Opt-in feature (doesn't affect existing users)

**Implementation:**
- `sync.Pool` for Message structs
- Configurable via `go.message.pool.enable`
- Benchmarks to validate improvement

---

### RFC-0008: Feature-Gated Builds (Reduce Binary Size)
**Impact:** 3/5 | **Effort:** 4/5 | **Timeline:** 3 weeks

**Why Medium Priority:**
- Benefits users who don't need Schema Registry (~27MB savings)
- Requires build tag infrastructure
- Maintenance overhead (multiple build configurations)

**Approach:**
- Build tag: `-tags minimal` (kafka package only)
- Build tag: `-tags noschemaregistry` (exclude Schema Registry)
- CI must test all combinations

---

### RFC-0010: Add OpenTelemetry Integration
**Impact:** 4/5 | **Effort:** 4/5 | **Timeline:** 3-4 weeks

**Why Medium Priority:**
- Industry-standard observability
- Complements Prometheus metrics
- Distributed tracing for multi-service architectures

**Scope:**
- Trace producer send path (produce → ack)
- Trace consumer poll path (fetch → process)
- Propagate trace context via message headers

---

## Lower Priority (Fill-ins)

### RFC-0005: Remove Legacy Avro (heetch/avro) Dependency
**Impact:** 3/5 | **Effort:** 2/5 | **Timeline:** 1 week

**Why Lower Priority:**
- Technical debt cleanup
- Breaking change (requires major version)
- Users can already use hamba/avro/v2

**Approach:**
- Deprecation notice in v2.x
- Full removal in v3.0
- Migration guide for affected users

---

## RFC Details

Each RFC includes:
1. **Summary**: One paragraph explanation
2. **Motivation**: Why this change is needed
3. **Detailed Design**: Technical implementation
4. **Example Usage**: Before/after code samples
5. **Implementation Plan**: Phased approach with milestones
6. **Backward Compatibility**: Impact on existing users
7. **Alternatives Considered**: Other approaches and why they weren't chosen
8. **Open Questions**: Unresolved issues for community input

---

## Implementation Roadmap

### Q1 2026: Quick Wins
- ✅ RFC-0001: Split adminapi.go
- ✅ RFC-0004: Code coverage reporting
- ✅ RFC-0009: Enhanced error messages

**Goal:** Improve developer experience and code quality metrics

---

### Q2 2026: Observability Foundation
- ✅ RFC-0002: Structured logging (slog)
- ✅ RFC-0003: Metrics exporter (Prometheus)

**Goal:** Production-grade observability for users

---

### Q3 2026: Documentation & Performance
- ✅ RFC-0007: Architecture documentation site
- ✅ RFC-0006: Object pooling (opt-in)

**Goal:** Lower barrier to contribution, optimize for high-throughput users

---

### Q4 2026: Advanced Features
- ✅ RFC-0008: Feature-gated builds
- ✅ RFC-0010: OpenTelemetry integration

**Goal:** Flexibility (binary size) and enterprise observability

---

### 2027: Breaking Changes (v3.0)
- ✅ RFC-0005: Remove legacy Avro dependency

**Goal:** Clean up technical debt

---

## Success Metrics

### Code Quality
- Average file size < 500 LOC
- Code coverage > 75%
- Zero high-severity static analysis warnings

### User Experience
- Support issue resolution time < 48 hours
- Contributor PR review time < 7 days
- Documentation completeness score > 90%

### Performance
- Producer throughput: >1M msg/sec (benchmark)
- Consumer throughput: >1M msg/sec (benchmark)
- Memory allocations: <50 bytes per message (pooling enabled)

### Community
- Monthly active contributors > 10
- GitHub stars growth rate > 5% per quarter
- Conference talks/blog posts mentioning the library

---

## Risk Assessment

| RFC | Risk Level | Mitigation |
|-----|------------|------------|
| RFC-0001 | 🟢 Low | Pure refactoring, comprehensive tests |
| RFC-0002 | 🟡 Medium | Phased rollout, backward compatibility |
| RFC-0003 | 🟡 Medium | Opt-in feature, performance testing |
| RFC-0004 | 🟢 Low | CI-only change, no runtime impact |
| RFC-0005 | 🟡 Medium | Major version bump, migration guide |
| RFC-0006 | 🟡 Medium | Opt-in, benchmark validation |
| RFC-0007 | 🟢 Low | Documentation-only |
| RFC-0008 | 🔴 High | Build complexity, CI test matrix |
| RFC-0009 | 🟢 Low | Backward compatible, incremental |
| RFC-0010 | 🟡 Medium | Opt-in, trace overhead testing |

---

## Resource Requirements

### Engineering Time
- **Quick Wins** (RFC-0001, 0004, 0009): ~3 weeks total (1 engineer)
- **Strategic** (RFC-0002, 0003, 0007): ~7 weeks total (1 engineer)
- **Medium Priority** (RFC-0006, 0008, 0010): ~10 weeks total (1-2 engineers)
- **Lower Priority** (RFC-0005): ~1 week (1 engineer)

**Total**: ~21 weeks (~5 months) with 1-2 dedicated engineers

### Infrastructure
- CI/CD: GitHub Actions minutes (coverage, multi-platform builds)
- Documentation hosting: GitHub Pages or Netlify (free)
- Metrics backend: User-provided (Prometheus, OTel collector)

---

## Community Engagement

### RFC Process
1. **Draft**: Author creates RFC in `rfcs/` directory
2. **Review**: Community feedback via GitHub issues
3. **Approval**: Maintainers approve after discussion
4. **Implementation**: Author or volunteer implements
5. **Release**: Include in next minor/major version

### Communication Channels
- **RFC discussions**: GitHub Discussions
- **Implementation PRs**: Link to RFC in PR description
- **Release notes**: Highlight RFC-driven changes

---

## Conclusion

These 10 RFCs represent **high-impact, achievable improvements** to confluent-kafka-go over the next 12-18 months. By prioritizing Quick Wins first, we build momentum and demonstrate value while planning larger Strategic initiatives.

**Recommended Starting Point**: RFC-0001 (Split adminapi.go) — immediate code quality improvement with minimal risk.

**Questions?** Each RFC has detailed design documentation. Review individual RFCs for implementation specifics.

---

**Next Steps:**
1. Review individual RFCs in this directory
2. Provide feedback via GitHub issues
3. Volunteer to implement (many RFCs are contributor-friendly!)
4. Track progress via RFC status updates

---

**RFC Status Legend:**
- 📝 **Draft**: Under discussion
- ✅ **Approved**: Ready for implementation
- 🚧 **In Progress**: Being implemented
- ✅ **Completed**: Shipped in release
- ❌ **Rejected**: Not proceeding
