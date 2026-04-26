MODULE  := github.com/ani03sha/kv-fabric
BIN_DIR := bin

GO      := go
GOFLAGS := -trimpath
LDFLAGS := -ldflags="-s -w"

SCENARIOS_BIN := $(BIN_DIR)/scenarios
BENCH_BIN     := $(BIN_DIR)/benchmark
KVCTL_BIN     := $(BIN_DIR)/kvctl

.PHONY: all build scenarios bench test test-unit vet clean \
		docker-build docker-up docker-down \
		scenario-phantom scenario-booking scenario-mvcc-bloat scenario-dirty-read

all: build

## ── Build ────────────────────────────────────────────────────────────────────

build:
	@mkdir -p $(BIN_DIR)
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $(SCENARIOS_BIN) ./cmd/scenarios
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $(BENCH_BIN)     ./cmd/benchmark
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $(KVCTL_BIN)     ./cmd/kvctl
	@echo "built: $(SCENARIOS_BIN)  $(BENCH_BIN)  $(KVCTL_BIN)"

## ── Scenarios ────────────────────────────────────────────────────────────────

# Run all 4 failure scenarios in sequence.
scenarios: build
	./$(SCENARIOS_BIN) all

scenario-phantom: build
	./$(SCENARIOS_BIN) phantom

scenario-booking: build
	./$(SCENARIOS_BIN) booking

scenario-mvcc-bloat: build
	./$(SCENARIOS_BIN) mvcc-bloat

scenario-dirty-read: build
	./$(SCENARIOS_BIN) dirty-read

## ── Benchmark ────────────────────────────────────────────────────────────────

# 80-run matrix: 5 workloads × 4 modes × 4 concurrencies. Takes ~4 minutes.
bench: build
	./$(BENCH_BIN)

## ── Tests ────────────────────────────────────────────────────────────────────

# Integration tests: spin up real in-process clusters, verify distributed guarantees.
# -race: data race detector (mandatory — this project is all goroutines and channels).
# -timeout 120s: each test starts a 3-node cluster; election + apply adds ~300ms per test.
test:
	$(GO) test -v -race -timeout 120s ./tests/...

# Unit tests for the store, replication, and consistency packages in isolation.
test-unit:
	$(GO) test -v -race -timeout 60s ./store/... ./replication/... ./consistency/...

## ── Code quality ─────────────────────────────────────────────────────────────

vet:
	$(GO) vet ./...

## ── Docker ───────────────────────────────────────────────────────────────────

docker-build:
	docker build -t kv-fabric:latest -f docker/Dockerfile .

# Run all scenarios inside Docker (uses the in-process cluster — no real networking needed).
docker-scenarios:
	docker-compose -f docker/docker-compose.yml run --rm runner ./scenarios all

# Run the benchmark inside Docker.
docker-bench:
	docker-compose -f docker/docker-compose.yml run --rm runner ./benchmark

docker-down:
	docker-compose -f docker/docker-compose.yml down -v

## ── Clean ────────────────────────────────────────────────────────────────────

clean:
	rm -rf $(BIN_DIR)
	$(GO) clean -testcache