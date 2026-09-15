#include <algorithm>
#include <atomic>
#include <chrono>
#include <climits>
#include <condition_variable>
#include <functional>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <queue>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#include <cmath>
#include <fstream>

using namespace std;
using Clock = chrono::steady_clock;

static constexpr long long NS_PER_SEC = 1000000000LL;

long long nowNs() {
    return chrono::duration_cast<chrono::nanoseconds>(
        Clock::now().time_since_epoch()
    ).count();
}

// ============================================================
// Corrected Token Bucket
// ============================================================

class TokenBucket {
private:
    double tokens;
    unsigned int capacity;
    unsigned int refill_rate;
    long long last_time;
    mutex mtx;

public:
    TokenBucket(unsigned int cap, unsigned int rate)
        : tokens(static_cast<double>(cap)),
          capacity(cap),
          refill_rate(rate),
          last_time(nowNs()) {
        if (cap == 0 || rate == 0) {
            throw invalid_argument(
                "TokenBucket capacity/refill rate must be > 0"
            );
        }
    }

    bool allowRequest() {
        lock_guard<mutex> lock(mtx);

        long long current = nowNs();
        long long diff = current - last_time;
        last_time = current;

        double added =
            (static_cast<double>(diff) / NS_PER_SEC) *
            static_cast<double>(refill_rate);

        tokens = min(
            static_cast<double>(capacity),
            tokens + added
        );

        if (tokens >= 1.0) {
            tokens -= 1.0;
            return true;
        }

        return false;
    }
};

// ============================================================
// Corrected Queue Processor
// ============================================================

class QueueProcess {
private:
    queue<function<void()>> q;
    long long interval_ns;
    size_t max_queue_size;

    Clock::time_point next_exec_time;

    mutable mutex mtx;
    condition_variable cv;

    bool stopped = false;
    size_t peak_size = 0;

public:
    QueueProcess(unsigned int rate, size_t max_size)
        : interval_ns(NS_PER_SEC / rate),
          max_queue_size(max_size),
          next_exec_time(Clock::now()) {
        if (rate == 0 || max_size == 0) {
            throw invalid_argument(
                "QueueProcess rate/capacity must be > 0"
            );
        }

        if (interval_ns == 0) {
            interval_ns = 1;
        }
    }

    void processor() {
        while (true) {
            function<void()> fn;

            {
                unique_lock<mutex> lock(mtx);

                cv.wait(lock, [this]() {
                    return !q.empty() || stopped;
                });

                if (stopped && q.empty()) {
                    return;
                }

                fn = move(q.front());
                q.pop();
            }

            Clock::time_point current = Clock::now();

            if (current < next_exec_time) {
                this_thread::sleep_until(next_exec_time);
            }

            fn();

            next_exec_time +=
                chrono::nanoseconds(interval_ns);

            current = Clock::now();

            if (next_exec_time < current) {
                next_exec_time = current;
            }
        }
    }

    bool newRequest(function<void()> fn) {
        {
            lock_guard<mutex> lock(mtx);

            if (stopped) {
                return false;
            }

            if (q.size() >= max_queue_size) {
                return false;
            }

            q.push(move(fn));

            if (q.size() > peak_size) {
                peak_size = q.size();
            }
        }

        cv.notify_one();
        return true;
    }

    void stop() {
        {
            lock_guard<mutex> lock(mtx);
            stopped = true;
        }

        cv.notify_all();
    }

    size_t peakQueueSize() const {
        lock_guard<mutex> lock(mtx);
        return peak_size;
    }
};

// ============================================================
// Traffic trace
//
// The important part:
// BOTH algorithms receive the exact same trace.
//
// Profiles:
//   1. Baseline      : Poisson 5k/s for 1s
//   2. Normal        : Poisson 8k/s for 1s
//   3. Flash burst   : 5,000 requests immediately
//   4. Recovery      : Poisson 7k/s for 1s
//   5. Overload      : Poisson 20k/s for 0.5s
//   6. Cooldown      : Poisson 3k/s for 1s
//
// Default limit:
//   Token bucket refill = 10k/s, capacity = 1k
//   Queue service       = 10k/s, capacity = 5k
//
// This creates:
//   - normal traffic
//   - true instantaneous bursts
//   - sustained overload
//   - recovery after overload
// ============================================================

enum class PhaseType {
    POISSON,
    BURST
};

struct Phase {
    string name;
    PhaseType type;
    double rate;
    long long duration_ns;
    long long burst_count;
};

struct RequestEvent {
    long long offset_ns;
    int phase;
    long long sequence;
};

struct PhaseStats {
    long long generated = 0;
    long long accepted = 0;
    long long rejected = 0;

    vector<double> latency_us;
};

struct BenchmarkResult {
    string name;

    long long generated = 0;
    long long accepted = 0;
    long long rejected = 0;
    long long completed = 0;

    double trace_seconds = 0.0;
    double submission_seconds = 0.0;
    double total_seconds = 0.0;
    double completion_throughput = 0.0;

    size_t peak_queue = 0;

    vector<PhaseStats> phases;
};

long long secondsToNs(double seconds) {
    long double ns =
        static_cast<long double>(seconds) *
        static_cast<long double>(NS_PER_SEC);

    if (ns < 1.0L) {
        return 1;
    }

    if (ns > static_cast<long double>(LLONG_MAX)) {
        throw overflow_error("Traffic phase too long");
    }

    return static_cast<long long>(ns);
}

vector<RequestEvent> buildTrafficTrace(
    const vector<Phase>& phases,
    unsigned int seed
) {
    mt19937_64 rng(seed);

    vector<RequestEvent> trace;

    long long phase_start = 0;
    long long sequence = 0;

    for (size_t phase_id = 0;
         phase_id < phases.size();
         ++phase_id) {

        const Phase& phase = phases[phase_id];

        if (phase.type == PhaseType::BURST) {
            for (long long i = 0;
                 i < phase.burst_count;
                 ++i) {

                trace.push_back({
                    phase_start,
                    static_cast<int>(phase_id),
                    sequence++
                });
            }
        } else {
            exponential_distribution<double> distribution(
                phase.rate
            );

            long long offset = 0;

            while (true) {
                double gap_seconds = distribution(rng);

                long long gap_ns =
                    secondsToNs(gap_seconds);

                offset += gap_ns;

                if (offset >= phase.duration_ns) {
                    break;
                }

                trace.push_back({
                    phase_start + offset,
                    static_cast<int>(phase_id),
                    sequence++
                });
            }
        }

        phase_start += phase.duration_ns;
    }

    sort(
        trace.begin(),
        trace.end(),
        [](const RequestEvent& a, const RequestEvent& b) {
            if (a.offset_ns != b.offset_ns) {
                return a.offset_ns < b.offset_ns;
            }

            return a.sequence < b.sequence;
        }
    );

    return trace;
}

// ============================================================
// Statistics
// ============================================================

double percentile(
    vector<double> values,
    double p
) {
    if (values.empty()) {
        return 0.0;
    }

    sort(values.begin(), values.end());

    double index =
        (p / 100.0) *
        static_cast<double>(values.size() - 1);

    size_t low = static_cast<size_t>(floor(index));
    size_t high = static_cast<size_t>(ceil(index));

    if (low == high) {
        return values[low];
    }

    double fraction = index - static_cast<double>(low);

    return values[low] +
           fraction * (values[high] - values[low]);
}

void printPercentiles(
    const string& label,
    const vector<double>& samples
) {
    cout << "    " << label
         << " p50/p95/p99: "
         << fixed << setprecision(2)
         << percentile(samples, 50.0) << " / "
         << percentile(samples, 95.0) << " / "
         << percentile(samples, 99.0)
         << " us\n";
}

// ============================================================
// Token Bucket replay
// ============================================================

BenchmarkResult runTokenBucket(
    const vector<RequestEvent>& trace,
    const vector<Phase>& phases,
    unsigned int capacity,
    unsigned int refill_rate
) {
    TokenBucket bucket(capacity, refill_rate);

    BenchmarkResult result;
    result.name = "TokenBucket";
    result.phases.resize(phases.size());

    Clock::time_point replay_start = Clock::now();

    long long last_offset = 0;

    for (const RequestEvent& event : trace) {
        Clock::time_point target =
            replay_start +
            chrono::nanoseconds(event.offset_ns);

        this_thread::sleep_until(target);

        Clock::time_point decision_start = Clock::now();

        bool accepted = bucket.allowRequest();

        Clock::time_point decision_end = Clock::now();

        double decision_us =
            chrono::duration<double, micro>(
                decision_end - decision_start
            ).count();

        PhaseStats& stats =
            result.phases[event.phase];

        ++stats.generated;

        if (accepted) {
            ++stats.accepted;
        } else {
            ++stats.rejected;
        }

        stats.latency_us.push_back(decision_us);

        last_offset = event.offset_ns;
    }

    Clock::time_point end = Clock::now();

    result.generated =
        static_cast<long long>(trace.size());

    for (const PhaseStats& stats : result.phases) {
        result.accepted += stats.accepted;
        result.rejected += stats.rejected;
    }

    result.completed = result.accepted;

    result.trace_seconds =
        static_cast<double>(last_offset) /
        NS_PER_SEC;

    result.submission_seconds =
        chrono::duration<double>(
            end - replay_start
        ).count();

    result.total_seconds =
        result.submission_seconds;

    if (result.total_seconds > 0.0) {
        result.completion_throughput =
            static_cast<double>(result.completed) /
            result.total_seconds;
    }

    return result;
}

// ============================================================
// Queue Process replay
// ============================================================

BenchmarkResult runQueueProcess(
    const vector<RequestEvent>& trace,
    const vector<Phase>& phases,
    unsigned int service_rate,
    size_t queue_capacity
) {
    QueueProcess processor(
        service_rate,
        queue_capacity
    );

    BenchmarkResult result;
    result.name = "QueueProcess";
    result.phases.resize(phases.size());

    atomic<long long> completed(0);

    thread worker([&processor]() {
        processor.processor();
    });

    Clock::time_point replay_start = Clock::now();

    long long last_offset = 0;

    for (const RequestEvent& event : trace) {
        Clock::time_point target =
            replay_start +
            chrono::nanoseconds(event.offset_ns);

        this_thread::sleep_until(target);

        Clock::time_point arrival = Clock::now();

        PhaseStats& stats =
            result.phases[event.phase];

        ++stats.generated;

        bool accepted =
            processor.newRequest(
                [&, phase = event.phase, arrival]() {
                    Clock::time_point completion =
                        Clock::now();

                    double wait_us =
                        chrono::duration<double, micro>(
                            completion - arrival
                        ).count();

                    result.phases[phase]
                        .latency_us
                        .push_back(wait_us);

                    completed.fetch_add(1);
                }
            );

        if (accepted) {
            ++stats.accepted;
        } else {
            ++stats.rejected;
        }

        last_offset = event.offset_ns;
    }

    Clock::time_point submission_end = Clock::now();

    // Graceful shutdown: finish all accepted work.
    processor.stop();
    worker.join();

    Clock::time_point total_end = Clock::now();

    result.generated =
        static_cast<long long>(trace.size());

    for (const PhaseStats& stats : result.phases) {
        result.accepted += stats.accepted;
        result.rejected += stats.rejected;
    }

    result.completed = completed.load();

    result.trace_seconds =
        static_cast<double>(last_offset) /
        NS_PER_SEC;

    result.submission_seconds =
        chrono::duration<double>(
            submission_end - replay_start
        ).count();

    result.total_seconds =
        chrono::duration<double>(
            total_end - replay_start
        ).count();

    result.completion_throughput =
        result.total_seconds > 0.0
            ? static_cast<double>(result.completed) /
              result.total_seconds
            : 0.0;

    result.peak_queue =
        processor.peakQueueSize();

    return result;
}

// ============================================================
// Output
// ============================================================

void printTraffic(
    const vector<Phase>& phases,
    const vector<RequestEvent>& trace
) {
    cout << "\n========================================\n";
    cout << "Traffic Trace\n";
    cout << "========================================\n";

    cout << "Total generated events : "
         << trace.size() << "\n\n";

    for (size_t i = 0; i < phases.size(); ++i) {
        long long count = 0;

        for (const RequestEvent& event : trace) {
            if (event.phase == static_cast<int>(i)) {
                ++count;
            }
        }

        cout << setw(12)
             << phases[i].name
             << " : "
             << count
             << " requests\n";
    }
}

void printResult(
    const BenchmarkResult& result,
    const vector<Phase>& phases
) {
    cout << "\n========================================\n";
    cout << result.name << "\n";
    cout << "========================================\n";

    cout << "Generated           : "
         << result.generated << "\n";

    cout << "Accepted            : "
         << result.accepted << "\n";

    cout << "Rejected            : "
         << result.rejected << "\n";

    cout << "Completed           : "
         << result.completed << "\n";

    cout << fixed << setprecision(4);

    cout << "Trace duration      : "
         << result.trace_seconds
         << " s\n";

    cout << "Submission duration : "
         << result.submission_seconds
         << " s\n";

    cout << "Total duration      : "
         << result.total_seconds
         << " s\n";

    cout << "Completion throughput: "
         << result.completion_throughput
         << " tasks/s\n";

    if (result.name == "QueueProcess") {
        cout << "Peak queue size     : "
             << result.peak_queue
             << "\n";
    }

    cout << "\nPhase results:\n";

    for (size_t i = 0; i < phases.size(); ++i) {
        const PhaseStats& stats = result.phases[i];

        double acceptance =
            stats.generated > 0
                ? static_cast<double>(stats.accepted) /
                  stats.generated * 100.0
                : 0.0;

        cout << "\n  " << phases[i].name << "\n";

        cout << "    generated/accepted/rejected: "
             << stats.generated << " / "
             << stats.accepted << " / "
             << stats.rejected << "\n";

        cout << "    acceptance: "
             << fixed << setprecision(2)
             << acceptance
             << "%\n";

        if (result.name == "TokenBucket") {
            printPercentiles(
                "decision latency",
                stats.latency_us
            );
        } else {
            printPercentiles(
                "queue wait",
                stats.latency_us
            );
        }
    }
}

void writeCsv(
    const string& filename,
    const vector<Phase>& phases,
    const BenchmarkResult& token,
    const BenchmarkResult& queue
) {
    ofstream output(filename);

    output
        << "system,phase,generated,accepted,rejected,"
        << "acceptance_pct,p50_us,p95_us,p99_us\n";

    for (size_t i = 0; i < phases.size(); ++i) {
        const PhaseStats& a = token.phases[i];

        double a_acceptance =
            a.generated > 0
                ? 100.0 * a.accepted / a.generated
                : 0.0;

        output
            << "TokenBucket,"
            << phases[i].name << ","
            << a.generated << ","
            << a.accepted << ","
            << a.rejected << ","
            << a_acceptance << ","
            << percentile(a.latency_us, 50.0) << ","
            << percentile(a.latency_us, 95.0) << ","
            << percentile(a.latency_us, 99.0)
            << "\n";

        const PhaseStats& b = queue.phases[i];

        double b_acceptance =
            b.generated > 0
                ? 100.0 * b.accepted / b.generated
                : 0.0;

        output
            << "QueueProcess,"
            << phases[i].name << ","
            << b.generated << ","
            << b.accepted << ","
            << b.rejected << ","
            << b_acceptance << ","
            << percentile(b.latency_us, 50.0) << ","
            << percentile(b.latency_us, 95.0) << ","
            << percentile(b.latency_us, 99.0)
            << "\n";
    }
}

// ============================================================
// Main
//
// Arguments:
//   1 = token refill/service rate
//   2 = token capacity
//   3 = queue capacity
//   4 = random seed
//
// Example:
//   ./benchmark_traffic 10000 1000 5000 42
// ============================================================

int main(int argc, char* argv[]) {
    unsigned int rate = 10000;
    unsigned int token_capacity = 1000;
    size_t queue_capacity = 5000;
    unsigned int seed = 42;

    if (argc >= 2) {
        rate = stoul(argv[1]);
    }

    if (argc >= 3) {
        token_capacity = stoul(argv[2]);
    }

    if (argc >= 4) {
        queue_capacity = stoul(argv[3]);
    }

    if (argc >= 5) {
        seed = stoul(argv[4]);
    }

    if (rate == 0 ||
        token_capacity == 0 ||
        queue_capacity == 0) {
        throw invalid_argument(
            "Rates and capacities must be > 0"
        );
    }

    vector<Phase> phases = {
        {
            "baseline",
            PhaseType::POISSON,
            5000.0,
            secondsToNs(1.0),
            0
        },
        {
            "normal",
            PhaseType::POISSON,
            8000.0,
            secondsToNs(1.0),
            0
        },
        {
            "flash_burst",
            PhaseType::BURST,
            0.0,
            secondsToNs(0.1),
            5000
        },
        {
            "recovery",
            PhaseType::POISSON,
            7000.0,
            secondsToNs(1.0),
            0
        },
        {
            "overload",
            PhaseType::POISSON,
            20000.0,
            secondsToNs(0.5),
            0
        },
        {
            "cooldown",
            PhaseType::POISSON,
            3000.0,
            secondsToNs(1.0),
            0
        }
    };

    vector<RequestEvent> trace =
        buildTrafficTrace(phases, seed);

    cout << "========================================\n";
    cout << "Realistic Traffic Rate-Limiter Benchmark\n";
    cout << "========================================\n";

    cout << "Rate limit         : "
         << rate << " req/s\n";

    cout << "Token capacity     : "
         << token_capacity << "\n";

    cout << "Queue capacity     : "
         << queue_capacity << "\n";

    cout << "Random seed        : "
         << seed << "\n";

    cout << "Traffic model      : "
         << "Poisson + flash burst + overload\n";

    printTraffic(phases, trace);

    cout << "\nRunning TokenBucket...\n";

    BenchmarkResult token_result =
        runTokenBucket(
            trace,
            phases,
            token_capacity,
            rate
        );

    cout << "Running QueueProcess...\n";

    BenchmarkResult queue_result =
        runQueueProcess(
            trace,
            phases,
            rate,
            queue_capacity
        );

    printResult(token_result, phases);
    printResult(queue_result, phases);

    cout << "\n========================================\n";
    cout << "Summary\n";
    cout << "========================================\n";

    double token_acceptance =
        token_result.generated > 0
            ? 100.0 * token_result.accepted /
              token_result.generated
            : 0.0;

    double queue_acceptance =
        queue_result.generated > 0
            ? 100.0 * queue_result.accepted /
              queue_result.generated
            : 0.0;

    cout << fixed << setprecision(2);

    cout << "TokenBucket acceptance : "
         << token_acceptance << "%\n";

    cout << "Queue acceptance      : "
         << queue_acceptance << "%\n";

    cout << "TokenBucket rejected  : "
         << token_result.rejected << "\n";

    cout << "Queue rejected        : "
         << queue_result.rejected << "\n";

    cout << "Queue peak size       : "
         << queue_result.peak_queue << "\n";

    cout << "Queue total latency   : "
         << queue_result.total_seconds
         << " s\n";

    cout << "\nCSV: traffic_benchmark.csv\n";

    writeCsv(
        "traffic_benchmark.csv",
        phases,
        token_result,
        queue_result
    );

    return 0;
}
