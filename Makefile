# Makefile for confluent-kafka-go
# Provides different build configurations for optimizing binary size

.PHONY: all build build-minimal build-noschemaregistry test test-all test-minimal test-noschemaregistry clean help

# Default target - full build with all features
all: build

# Full build with all features (~35MB)
# Includes: Kafka + Schema Registry + Rules + Encryption
build:
	@echo "Building with all features (full build)..."
	go build ./...
	@echo "Build complete. Binary includes all features."

# Minimal build - Kafka only (~8MB)
# Excludes: Schema Registry, Rules, Encryption
build-minimal:
	@echo "Building minimal version (Kafka only)..."
	go build -tags minimal ./kafka/...
	@echo "Minimal build complete. Schema Registry, rules, and encryption excluded."

# Build without Schema Registry (~12MB)
# Excludes: Schema Registry only
# Includes: Kafka, some utilities
build-noschemaregistry:
	@echo "Building without Schema Registry..."
	go build -tags noschemaregistry ./kafka/...
	@echo "Build complete. Schema Registry excluded."

# Run all tests with full features
test:
	@echo "Running tests with all features..."
	go test -v ./...

# Run all tests for all build configurations
test-all: test test-minimal test-noschemaregistry
	@echo "All build configurations tested successfully."

# Test minimal build
test-minimal:
	@echo "Testing minimal build..."
	go test -v -tags minimal ./kafka/...

# Test noschemaregistry build
test-noschemaregistry:
	@echo "Testing noschemaregistry build..."
	go test -v -tags noschemaregistry ./kafka/...

# Verify binary sizes (informational)
size-check:
	@echo "Building and checking binary sizes..."
	@echo "\n=== Full Build ==="
	@go build -o /tmp/confluent-kafka-go-full ./kafka && ls -lh /tmp/confluent-kafka-go-full | awk '{print "Size: " $$5}' && rm /tmp/confluent-kafka-go-full
	@echo "\n=== Minimal Build ==="
	@go build -tags minimal -o /tmp/confluent-kafka-go-minimal ./kafka && ls -lh /tmp/confluent-kafka-go-minimal | awk '{print "Size: " $$5}' && rm /tmp/confluent-kafka-go-minimal
	@echo "\n=== No Schema Registry Build ==="
	@go build -tags noschemaregistry -o /tmp/confluent-kafka-go-nosr ./kafka && ls -lh /tmp/confluent-kafka-go-nosr | awk '{print "Size: " $$5}' && rm /tmp/confluent-kafka-go-nosr

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	go clean ./...
	@echo "Clean complete."

# Show help
help:
	@echo "confluent-kafka-go Build System"
	@echo "================================"
	@echo ""
	@echo "Build Targets:"
	@echo "  make build                 - Full build with all features (~35MB)"
	@echo "  make build-minimal         - Minimal build, Kafka only (~8MB)"
	@echo "  make build-noschemaregistry- Build without Schema Registry (~12MB)"
	@echo ""
	@echo "Test Targets:"
	@echo "  make test                  - Run tests with all features"
	@echo "  make test-minimal          - Run tests for minimal build"
	@echo "  make test-noschemaregistry - Run tests for noschemaregistry build"
	@echo "  make test-all              - Run tests for all configurations"
	@echo ""
	@echo "Other Targets:"
	@echo "  make size-check            - Build and compare binary sizes"
	@echo "  make clean                 - Clean build artifacts"
	@echo "  make help                  - Show this help message"
	@echo ""
	@echo "Build Tags:"
	@echo "  -tags minimal              - Exclude Schema Registry, rules, encryption"
	@echo "  -tags noschemaregistry     - Exclude Schema Registry only"
	@echo ""
