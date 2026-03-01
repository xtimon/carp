#!/usr/bin/env bash
# Run a 3-node cluster - start each node in a separate terminal.

set -e
cd "$(dirname "$0")"

# Build if needed
if [ ! -f ./carp ]; then
  go build -o carp ./cmd/server
fi
if [ ! -f ./carp-cli ]; then
  go build -o carp-cli ./cmd/cli
fi
if [ ! -f ./carp-bench ]; then
  go build -o carp-bench ./cmd/bench
fi

echo "CARP - 3-node cluster"
echo ""
echo "Start each node in a separate terminal:"
echo ""
echo "  Terminal 1: ./carp --config config/node1.yaml"
echo "  Terminal 2: ./carp --config config/node2.yaml"
echo "  Terminal 3: ./carp --config config/node3.yaml"
echo ""
echo "You should see 'Discovered ...' logs when nodes find each other."
echo "Connect: redis-cli -p 6379 (or 6380, 6381)"
echo "Or use ring-aware CLI: ./carp-cli -h 127.0.0.1:6379,127.0.0.1:6380,127.0.0.1:6381"
echo "Benchmark: ./carp-bench -h 127.0.0.1:6379,127.0.0.1:6380,127.0.0.1:6381"
echo ""

# If invoked with --start, launch node 1
if [ "$1" = "--start" ]; then
  exec ./carp --config config/node1.yaml
fi
