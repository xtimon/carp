.PHONY: build test test-cover bench run run-cluster clean install deps

BINARY := carp
CLI := carp-cli
BENCH := carp-bench

# Build all binaries
build:
	go build -o $(BINARY) ./cmd/server
	go build -o $(CLI) ./cmd/cli
	go build -o $(BENCH) ./cmd/bench

# Run tests
test:
	go test ./...

# Run integration tests (6-node cluster, 3 racks x 2 nodes)
test-integration:
	go test -tags=integration ./integration -v -timeout 90s

# Run tests with coverage
test-cover:
	go test -cover ./...

# Run performance benchmarks (Go benchmark tests)
bench:
	go test -bench=. -benchmem ./internal/storage/... ./internal/coordinator/... ./internal/resp/...

# Run single-node server
run:
	@go build -o $(BINARY) ./cmd/server
	./$(BINARY) --config config/single-node.yaml

# Start node 1 (for cluster - run node2 and node3 in other terminals)
run-cluster:
	@$(MAKE) build
	@./run_cluster.sh --start

# Clean build artifacts
clean:
	rm -f $(BINARY) $(CLI) $(BENCH)

# Install binaries to GOPATH/bin
install:
	go install ./cmd/server
	go install ./cmd/cli
	go install ./cmd/bench

# Download dependencies
deps:
	go mod download
