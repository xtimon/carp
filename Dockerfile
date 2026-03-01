# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy dependency files first for better layer caching
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /carp ./cmd/server

# Runtime stage
FROM alpine:3.19

RUN apk add --no-cache ca-certificates

WORKDIR /app

COPY --from=builder /carp .
COPY config/single-node.yaml ./config/

# Redis, gossip, RPC
EXPOSE 6379 7000 7379

# Bind to all interfaces inside container
ENV HOST=0.0.0.0

ENTRYPOINT ["./carp", "--config", "config/single-node.yaml"]
