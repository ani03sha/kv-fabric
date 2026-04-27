SERVER_BIN := $(BIN_DIR)/server
MODULE  := github.com/ani03sha/kv-fabric
BIN_DIR := bin

GO      := go
GOFLAGS := -trimpath
LDFLAGS := -ldflags="-s -w"

SCENARIOS_BIN := $(BIN_DIR)/scenarios
BENCH_BIN     := $(BIN_DIR)/benchmark
KVCTL_BIN     := $(BIN_DIR)/kvctl

.PHONY: all build scenarios bench test test-unit vet clean \
        cluster-start cluster-stop cluster-clean \
        docker-build docker-scenarios docker-bench \
        docker-cluster-start docker-cluster-stop docker-cluster-clean \
        scenario-phantom scenario-booking scenario-mvcc-bloat scenario-dirty-read

all: build

## --- Build ---

build:
	@mkdir -p $(BIN_DIR)
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $(SCENARIOS_BIN) ./cmd/scenarios
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $(BENCH_BIN)     ./cmd/benchmark
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $(KVCTL_BIN)     ./cmd/kvctl
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $(SERVER_BIN)    ./cmd/server
	@echo "built: $(SCENARIOS_BIN)  $(BENCH_BIN)  $(KVCTL_BIN)  $(SERVER_BIN)"

## --- Scenarios ---

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

## ── Local cluster ────────────────────────────────────────────────────────────

# Start a 3-node cluster on localhost in the background.
# Each node writes its PID to /tmp and logs to /tmp/kv-fabric-node-N.log.
# KV ports: 9001 (node-1), 9002 (node-2), 9003 (node-3).
cluster-start: build
	@mkdir -p data/node-1 data/node-2 data/node-3
	@$(SERVER_BIN) \
		--id node-1 --raft-addr :7001 --kv-addr :9001 \
		--raft-peers "node-2=:7002,node-3=:7003" \
		--kv-peers   "node-2=:9002,node-3=:9003" \
		--data-dir data/node-1 \
		>> /tmp/kv-fabric-node-1.log 2>&1 & echo $$! > /tmp/kv-fabric-node-1.pid
	@$(SERVER_BIN) \
		--id node-2 --raft-addr :7002 --kv-addr :9002 \
		--raft-peers "node-1=:7001,node-3=:7003" \
		--kv-peers   "node-1=:9001,node-3=:9003" \
		--data-dir data/node-2 \
		>> /tmp/kv-fabric-node-2.log 2>&1 & echo $$! > /tmp/kv-fabric-node-2.pid
	@$(SERVER_BIN) \
		--id node-3 --raft-addr :7003 --kv-addr :9003 \
		--raft-peers "node-1=:7001,node-2=:7002" \
		--kv-peers   "node-1=:9001,node-2=:9002" \
		--data-dir data/node-3 \
		>> /tmp/kv-fabric-node-3.log 2>&1 & echo $$! > /tmp/kv-fabric-node-3.pid
	@echo "cluster started — logs: /tmp/kv-fabric-node-{1,2,3}.log"

cluster-stop:
	@for f in /tmp/kv-fabric-node-1.pid /tmp/kv-fabric-node-2.pid /tmp/kv-fabric-node-3.pid; do \
		[ -f $$f ] && kill $$(cat $$f) 2>/dev/null && rm -f $$f || true; \
	done
	@echo "cluster stopped"

# cluster-clean stops the cluster and removes the WAL data directories.
cluster-clean: cluster-stop
	rm -rf data/

And add Docker cluster targets after the existing docker-down target:

# Bring up the 3-node containerised cluster.
docker-cluster-start:
	docker compose -f docker/docker-compose-cluster.yml up -d

# Stop the cluster, keep volumes.
docker-cluster-stop:
	docker compose -f docker/docker-compose-cluster.yml down

# Stop the cluster and delete all WAL volumes.
docker-cluster-clean:
	docker compose -f docker/docker-compose-cluster.yml down -v

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
	docker-compose -f docker/docker-compose.yml run --rm runner

# Run the benchmark inside Docker.
docker-bench:
	docker-compose -f docker/docker-compose.yml run --rm --entrypoint ./benchmark runner

docker-down:
	docker-compose -f docker/docker-compose.yml down -v

## ── Clean ────────────────────────────────────────────────────────────────────

clean:
	rm -rf $(BIN_DIR)
	$(GO) clean -testcache