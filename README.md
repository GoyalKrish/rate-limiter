
# Rate Limiter

Simple C++ implementations of common rate-limiting algorithms, along with benchmarks.

Rate limiters are more than simple allow/deny checks. Different algorithms make different trade-offs between latency, burst handling, and how strictly they protect resources.

This repository contains:

- **Token Bucket**
- **Queue-based processor** (Leaky Bucket style)

## Algorithms

### Token Bucket
- Tokens are added at a fixed rate up to a maximum capacity.
- A request is allowed only if at least one token is available.
- Supports bursts up to the capacity, then settles to the refill rate.
- Decision is very fast (usually sub-microsecond in these benchmarks).
- Excess requests are rejected immediately.

### QueueProcess (Leaky Bucket style)
- Incoming requests are placed in a queue.
- A worker processes them at a controlled rate.
- Can accept more requests under burst by queuing them.
- Introduces latency while requests wait in the queue.
- Protects the downstream system by smoothing traffic.

## Files

| File | Description |
|------|-------------|
| `RateLimiter.h` | Core implementations of TokenBucket and QueueProcess |
| `benchmark.cc` | Detailed benchmark comparing both approaches |
| `test.cc` | Example usage with the Crow HTTP framework |
| `monitor.sh` | Helper script for running benchmarks with system monitoring |
| `benchmark_results_*/` | Sample benchmark output |

## Building & Running

```bash
# Compile the benchmark
g++ -O2 -std=c++17 benchmark.cc -o benchmark -pthread

# Run with default parameters
./benchmark

# Or use the monitor script (example)
./monitor.sh <arrival_rate> <capacity> <refill_rate>
```

Example:
```bash
./monitor.sh 100000 1000 100000
```

## Example Results

Benchmarks were run with 10 million requests at high arrival rates.

**Token Bucket** (low capacity example):
- Extremely low decision latency (p50 often < 0.1 µs)
- Rejects the large majority of requests under sustained overload
- No queuing — decisions are immediate

**QueueProcess**:
- Accepts essentially all requests (within queue limits)
- Higher latency due to queuing (p50 in the low microseconds range in these runs)
- Smooths traffic for the downstream system

Exact numbers vary with capacity, refill rate, and arrival pattern. See the `benchmark_results_*` folders and `res.txt` for full output.

## Notes

- The implementations are intentionally straightforward for learning and comparison.
- Token Bucket is better when you want fast rejection and controlled bursts.
- The queue-based approach is better when you prefer to accept and smooth traffic rather than drop it.
- Both are single-process and use mutexes; they are not distributed rate limiters.

Feel free to experiment with different capacities, rates, and arrival patterns.