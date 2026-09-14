
#!/usr/bin/env bash

set -euo pipefail

BINARY="./benchmark"
ARRIVAL_RATE="${1:-100000}"
CAPACITY="${2:-10}"
REFILL_RATE="${3:-1}"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

OUT_DIR="benchmark_results_${TIMESTAMP}"

mkdir -p "$OUT_DIR"

LOG_FILE="$OUT_DIR/benchmark.log"
PIDSTAT_FILE="$OUT_DIR/pidstat.csv"
VMSTAT_FILE="$OUT_DIR/vmstat.csv"
TIME_FILE="$OUT_DIR/time.txt"

echo "========================================"
echo "Rate Limiter Benchmark Monitor"
echo "========================================"

echo "Output directory : $OUT_DIR"
echo "Arrival rate     : $ARRIVAL_RATE req/s"
echo "Capacity         : $CAPACITY"
echo "Refill rate      : $REFILL_RATE"
echo

echo "Starting benchmark..."

/usr/bin/time \
    -v \
    "$BINARY" \
    "$ARRIVAL_RATE" \
    "$CAPACITY" \
    "$REFILL_RATE" \
    > "$LOG_FILE" \
    2> "$TIME_FILE" &

BENCHMARK_PID=$!

echo "Benchmark PID: $BENCHMARK_PID"

echo "Starting CPU and memory monitoring..."

echo "timestamp,pid,%cpu,%mem,rss_kb,voluntary_ctxt_switches,nonvoluntary_ctxt_switches" \
    > "$PIDSTAT_FILE"

echo "timestamp,r,b,swpd,free,buff,cache,si,so,bi,bo,in,cs,us,sy,id,wa,st" \
    > "$VMSTAT_FILE"

while kill -0 "$BENCHMARK_PID" 2>/dev/null; do

    CURRENT_TIME=$(date +"%H:%M:%S")

# pidstat output:
# -u CPU
# -r Memory
# -w Context switches
# -p Specific PID
# -h Easy parsing

    PIDSTAT_OUTPUT=$(
        pidstat -u -r -w -p "$BENCHMARK_PID" 1 1 \
        | tail -n 1
    )

    echo "$CURRENT_TIME,$PIDSTAT_OUTPUT" \
        >> "$PIDSTAT_FILE"

    VMSTAT_OUTPUT=$(
        vmstat 1 2 \
        | tail -n 1
    )

    echo "$CURRENT_TIME,$VMSTAT_OUTPUT" \
        >> "$VMSTAT_FILE"

done

wait "$BENCHMARK_PID"

echo
echo "Benchmark finished."

echo "========================================"
echo "Results"
echo "========================================"

echo
echo "Benchmark log:"
cat "$LOG_FILE"

echo
echo "System resource usage:"
cat "$TIME_FILE"

echo
echo "Files saved in:"
echo "$OUT_DIR"