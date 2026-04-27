# Stage 1 — Build
# All three binaries are compiled here. CGO is disabled for a fully static binary
# that runs on any Linux without glibc.
FROM golang:1.26-alpine AS builder

WORKDIR /app

# Download dependencies first — cached as long as go.mod/go.sum don't change.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
        -o bin/scenarios ./cmd/scenarios

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
        -o bin/benchmark ./cmd/benchmark

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
        -o bin/kvctl ./cmd/kvctl

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
        -o bin/server ./cmd/server

# Stage 2 — Runtime
# Alpine gives us a shell for debugging. Scratch would be even smaller but loses
# the ability to exec into the container.
FROM alpine:3.20

WORKDIR /app

COPY --from=builder /app/bin/scenarios  ./scenarios
COPY --from=builder /app/bin/benchmark  ./benchmark
COPY --from=builder /app/bin/kvctl      ./kvctl
COPY --from=builder /app/bin/server ./server

# Default: run all scenarios.
# Override with: docker run kv-fabric:latest ./benchmark
ENTRYPOINT ["./scenarios"]
CMD ["all"]