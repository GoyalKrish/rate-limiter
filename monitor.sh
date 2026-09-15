#!/usr/bin/env bash

set -euo pipefail

BINARY="./benchmark"

RATE="${1:-10000}"
TOKEN_CAPACITY="${2:-1000}"
QUEUE_CAPACITY="${3:-5000}"
SEED="${4:-42}"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
OUT_DIR="traffic_benchmark_${TIMESTAMP}"

mkdir -p "$OUT_DIR"

LOG_FILE="$OUT_DIR/benchmark.log"
TIME_FILE="$OUT_DIR/time.txt"

echo "========================================"
echo "Realistic Traffic Benchmark Monitor"
echo "========================================"

echo "Output directory : $OUT_DIR"
echo "Rate limit       : $RATE req/s"
echo "Token capacity   : $TOKEN_CAPACITY"
echo "Queue capacity   : $QUEUE_CAPACITY"
echo "Random seed      : $SEED"
echo

if [[ ! -x "$BINARY" ]]; then
    echo "Error: $BINARY not found or not executable."
    echo "Compile with:"
    echo "  g++ -std=c++17 -O2 -pthread benchmark_traffic.cc -o benchmark_traffic"
    exit 1
fi

echo "Starting benchmark..."

/usr/bin/time \
    -v \
    "$BINARY" \
    "$RATE" \
    "$TOKEN_CAPACITY" \
    "$QUEUE_CAPACITY" \
    "$SEED" \
    > "$LOG_FILE" \
    2> "$TIME_FILE"

echo
echo "Benchmark finished."

echo "========================================"
echo "Results"
echo "========================================"

echo
cat "$LOG_FILE"

echo
echo "System resource usage:"
cat "$TIME_FILE"

echo
echo "Files saved in:"
echo "$OUT_DIR"
