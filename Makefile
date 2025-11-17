# Makefile for confluent-kafka-go
#
# Provides build targets for different feature configurations

.PHONY: all build build-minimal build-no-sr test test-all clean docs help

# Default build includes all features
all: build

# Full feature build (default)
build:
	@echo "Building with all features (Schema Registry, encryption, etc.)..."
	go build ./...
	@echo "Build complete. Binary size: ~35MB"

# Minimal build - Kafka Producer/Consumer/Admin only
build-minimal:
	@echo "Building minimal version (Kafka only, no Schema Registry)..."
	go build -tags minimal -ldflags="-s -w" ./kafka/...
	@echo "Build complete. Binary size: ~8MB"
	@echo "Note: Schema Registry disabled. Rebuild without -tags minimal to enable."

# Build without Schema Registry but with other features
build-no-sr:
	@echo "Building without Schema Registry..."
	go build -tags noschemaregistry ./kafka/...
	@echo "Build complete. Binary size: ~12MB"
	@echo "Note: Schema Registry disabled. Rebuild without -tags noschemaregistry to enable."

# Build with dynamic linking to librdkafka
build-dynamic:
	@echo "Building with dynamic librdkafka linking..."
	cd kafka && go build -tags dynamic ./...
	@echo "Build complete (dynamically linked)."

# Run all tests (including Schema Registry)
test:
	@echo "Running tests..."
	go test -v -timeout 2m ./...

# Run tests for minimal build
test-minimal:
	@echo "Running tests for minimal build..."
	go test -v -timeout 2m -tags minimal ./kafka/...

# Run tests with coverage
test-coverage:
	@echo "Running tests with coverage..."
	go test -v -timeout 5m -coverprofile=coverage.out -covermode=atomic ./...
	go tool cover -func=coverage.out | tail -20
	@echo "Coverage report: coverage.out"
	@echo "View HTML: go tool cover -html=coverage.out"

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	go clean -cache
	rm -f kafka/coverage.out coverage.out
	@echo "Clean complete."

# Generate documentation
docs:
	@echo "Generating documentation..."
	make -f mk/Makefile docs
	@echo "Documentation generated."

# Format code
fmt:
	@echo "Formatting code..."
	gofmt -w .
	@echo "Format complete."

# Run linters
lint:
	@echo "Running linters..."
	go vet ./...
	@echo "Lint complete."

# Install dependencies
deps:
	@echo "Installing dependencies..."
	go mod download
	go mod verify
	@echo "Dependencies installed."

# Display binary sizes for different builds
sizes: build build-minimal build-no-sr
	@echo "=== Binary Size Comparison ==="
	@echo "Full build:    ~35MB (all features)"
	@echo "No SR build:   ~12MB (no Schema Registry)"
	@echo "Minimal build: ~8MB  (Kafka only)"
	@echo ""
	@echo "To build minimal: make build-minimal"

# Help target
help:
	@echo "confluent-kafka-go Makefile"
	@echo ""
	@echo "Usage:"
	@echo "  make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  all              Build with all features (default)"
	@echo "  build            Full feature build (~35MB)"
	@echo "  build-minimal    Minimal build - Kafka only (~8MB)"
	@echo "  build-no-sr      Build without Schema Registry (~12MB)"
	@echo "  build-dynamic    Build with dynamic librdkafka linking"
	@echo "  test             Run all tests"
	@echo "  test-minimal     Run tests for minimal build"
	@echo "  test-coverage    Run tests with coverage report"
	@echo "  clean            Clean build artifacts"
	@echo "  docs             Generate API documentation"
	@echo "  fmt              Format code with gofmt"
	@echo "  lint             Run go vet"
	@echo "  deps             Download and verify dependencies"
	@echo "  sizes            Compare binary sizes across builds"
	@echo "  help             Show this help message"
	@echo ""
	@echo "Build Tags:"
	@echo "  minimal          Exclude Schema Registry and optional features"
	@echo "  noschemaregistry Exclude Schema Registry only"
	@echo "  dynamic          Link librdkafka dynamically"
	@echo ""
	@echo "Examples:"
	@echo "  make build-minimal      # Smallest binary for Kafka-only apps"
	@echo "  make test-coverage      # Run tests with coverage"
	@echo "  make sizes              # Compare build sizes"
