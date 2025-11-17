# RFC-0004: Add Code Coverage Reporting to CI

**Status:** Draft 📝
**Author:** Technical Analysis Team
**Created:** 2025-11-16
**Priority:** P0 (Quick Win)

## Summary

Add automated code coverage reporting to GitHub Actions CI pipeline using `go test -coverprofile` and integrate with CodeCov or Coveralls for visibility, PR comments, and coverage trend tracking.

## Motivation

**Current State:** No coverage metrics visible in CI
**Problem:** Cannot track coverage trends or enforce minimums
**Impact:** Potential regressions go unnoticed

## Detailed Design

### GitHub Actions Workflow

```yaml
# .github/workflows/coverage.yml
name: Code Coverage

on: [pull_request, push]

jobs:
  coverage:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Go
        uses: actions/setup-go@v4
        with:
          go-version: '1.24'
      
      - name: Run tests with coverage
        run: |
          go test -v -coverprofile=coverage.out -covermode=atomic ./...
          go tool cover -func=coverage.out
      
      - name: Upload to CodeCov
        uses: codecov/codecov-action@v3
        with:
          files: ./coverage.out
          flags: unittests
          name: codecov-umbrella
          fail_ci_if_error: true
```

### Coverage Targets

- **Minimum:** 70% (enforce in CI)
- **Target:** 80% (aspirational)
- **Per-package:** Track separately

## Implementation Plan

**Day 1:**
- Add coverage workflow to `.github/workflows/`
- Configure CodeCov account
- Add badge to README

**Day 2:**
- Set up coverage diff comments on PRs
- Configure coverage thresholds
- Test on sample PR

## Success Criteria

- ✅ Coverage visible in every PR
- ✅ Badge in README shows current coverage
- ✅ CI fails if coverage decreases significantly

**Effort:** 2 days
**Impact:** High (quality visibility)
