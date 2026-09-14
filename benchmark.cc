
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <functional>
#include <future>
#include <iomanip>
#include <iostream>
#include <limits>
#include <mutex>
#include <queue>
#include <random>
#include <string>
#include <thread>
#include <vector>
#include <cmath>

using namespace std;

using Clock = chrono::steady_clock;
using TimePoint = Clock::time_point;

static const uint64_t TOTAL_REQUESTS = 10000000ULL;

long long nowTime() {
    return chrono::duration_cast<chrono::milliseconds>(
        Clock::now().time_since_epoch()
    ).count();
}

uint64_t nsBetween(TimePoint start, TimePoint end) {
    return static_cast<uint64_t>(
        chrono::duration_cast<chrono::nanoseconds>(end - start).count()
    );
}

/*
    Shared request result.

    The benchmark measures:
        request start -> request completion

    This includes:
        - Lock acquisition
        - Queue waiting
        - Callback execution
        - Promise/future synchronization
*/
struct RequestResult {
    bool accepted;
    uint64_t latency_ns;
};

/*
    Metrics collected by each implementation.
*/
struct Metrics {
    atomic<uint64_t> completed;
    atomic<uint64_t> accepted;
    atomic<uint64_t> rejected;

    atomic<uint64_t> worker_busy_ns;
    atomic<uint64_t> worker_idle_ns;

    atomic<uint64_t> max_queue_depth;

    Metrics() {
        completed.store(0);
        accepted.store(0);
        rejected.store(0);
        worker_busy_ns.store(0);
        worker_idle_ns.store(0);
        max_queue_depth.store(0);
    }

    void record(const RequestResult& result) {
        completed.fetch_add(1, memory_order_relaxed);

        if (result.accepted) {
            accepted.fetch_add(1, memory_order_relaxed);
        } else {
            rejected.fetch_add(1, memory_order_relaxed);
        }
    }
};

/*
    Token Bucket

    Same basic algorithm as your implementation.

    capacity:
        Maximum number of tokens.

    refill_rate:
        Tokens added per second.

    Thread safety:
        One mutex protects the complete bucket state.
*/
class TokenBucket {
    double tokens;
    unsigned int capacity;
    unsigned int refill_rate;
    TimePoint last_time;

    mutex mtx;

public:
    TokenBucket(unsigned int cap, unsigned int rate) {
        tokens = cap;
        capacity = cap;
        refill_rate = rate;
        last_time = Clock::now();
    }

    bool allowRequest() {
        lock_guard<mutex> lock(mtx);

        TimePoint now_time = Clock::now();

        long long diff =
            chrono::duration_cast<chrono::milliseconds>(
                now_time - last_time
            ).count();

        last_time = now_time;

        double canAdd =
            (diff / 1000.0) * refill_rate;

        tokens = min(
            static_cast<double>(capacity),
            tokens + canAdd
        );

        if (tokens >= 1.0) {
            --tokens;
            return true;
        }

        return false;
    }
};

/*
    QueueProcess

    One worker thread.

    The worker does NOT sleep artificially.

    It waits only when there is no work.

    Worker busy time:
        Time spent executing callbacks.

    Worker idle time:
        Time spent waiting for work or waiting on an empty queue.

    This implementation also tracks the maximum queue depth.
*/
class QueueProcess {
public:
    queue<function<void()>> q;

    mutex mtx;
    condition_variable cv;

    bool stopped;

    Metrics* metrics;

    atomic<bool> running;

    QueueProcess(Metrics* m) {
        stopped = false;
        metrics = m;
        running.store(true);
    }

    void processor() {
        TimePoint idle_start = Clock::now();

        while (true) {
            function<void()> fn;

            {
                unique_lock<mutex> lock(mtx);

                /*
                    We have started waiting for work.
                */
                idle_start = Clock::now();

                cv.wait(lock, [this]() {
                    return !q.empty() || stopped;
                });

                /*
                    We woke up because work is available,
                    or because the queue is stopping.
                */
                TimePoint busy_start = Clock::now();

                uint64_t idle_ns =
                    nsBetween(idle_start, busy_start);

                metrics->worker_idle_ns.fetch_add(
                    idle_ns,
                    memory_order_relaxed
                );

                if (stopped && q.empty()) {
                    running.store(false);
                    return;
                }

                fn = q.front();
                q.pop();

                /*
                    The worker is now executing work.
                    The mutex is released before callback execution.
                */
            }

            TimePoint work_start = Clock::now();

            fn();

            TimePoint work_end = Clock::now();

            uint64_t busy_ns =
                nsBetween(work_start, work_end);

            metrics->worker_busy_ns.fetch_add(
                busy_ns,
                memory_order_relaxed
            );
        }
    }

    void newRequest(function<void()> fn) {
        uint64_t current_depth;

        {
            lock_guard<mutex> lock(mtx);

            q.push(fn);

            current_depth = q.size();
        }

        uint64_t old_depth =
            metrics->max_queue_depth.load(
                memory_order_relaxed
            );

        while (
            current_depth > old_depth &&
            !metrics->max_queue_depth.compare_exchange_weak(
                old_depth,
                current_depth,
                memory_order_relaxed
            )
        ) {
        }

        cv.notify_one();
    }

    void stop() {
        {
            lock_guard<mutex> lock(mtx);
            stopped = true;
        }

        cv.notify_all();
    }
};

/*
    Request arrival schedule.

    Generates exponential inter-arrival times.

    If arrival_rate = 100000:
        Mean interval = 1 / 100000 seconds
                     = 10 microseconds

    The same schedule is used for both implementations.
*/
vector<uint64_t> generateArrivals(
    uint64_t count,
    double arrival_rate,
    uint64_t seed
) {
    vector<uint64_t> arrivals;
    arrivals.reserve(count);

    mt19937_64 rng(seed);

    exponential_distribution<double> distribution(
        arrival_rate
    );

    uint64_t current_ns = 0;

    for (uint64_t i = 0; i < count; ++i) {
        double interval_seconds = distribution(rng);

        uint64_t interval_ns =
            static_cast<uint64_t>(
                interval_seconds * 1000000000.0
            );

        current_ns += interval_ns;

        arrivals.push_back(current_ns);
    }

    return arrivals;
}

/*
    Percentile calculation.

    Input must be sorted.
*/
double percentile(
    const vector<uint64_t>& values,
    double p
) {
    if (values.empty()) {
        return 0.0;
    }

    size_t index =
        static_cast<size_t>(
            p * static_cast<double>(values.size() - 1)
        );

    return static_cast<double>(values[index]) / 1000.0;
}

/*
    Benchmark output.

    Latencies are converted:
        ns -> microseconds
*/
void printReport(
    const string& name,
    const Metrics& metrics,
    vector<uint64_t>& latencies,
    double wall_seconds
) {
    sort(latencies.begin(), latencies.end());

    uint64_t completed =
        metrics.completed.load();

    uint64_t accepted =
        metrics.accepted.load();

    uint64_t rejected =
        metrics.rejected.load();

    uint64_t busy_ns =
        metrics.worker_busy_ns.load();

    uint64_t idle_ns =
        metrics.worker_idle_ns.load();

    uint64_t max_depth =
        metrics.max_queue_depth.load();

    double throughput =
        static_cast<double>(completed) / wall_seconds;

    double busy_seconds =
        static_cast<double>(busy_ns) / 1000000000.0;

    double idle_seconds =
        static_cast<double>(idle_ns) / 1000000000.0;

    double utilization = 0.0;

    if (busy_seconds + idle_seconds > 0.0) {
        utilization =
            busy_seconds /
            (busy_seconds + idle_seconds) *
            100.0;
    }

    cout << "\n";
    cout << "========================================\n";
    cout << name << "\n";
    cout << "========================================\n";

    cout << fixed << setprecision(3);

    cout << "Total requests       : "
         << TOTAL_REQUESTS << "\n";

    cout << "Completed            : "
         << completed << "\n";

    cout << "Accepted             : "
         << accepted << "\n";

    cout << "Rejected             : "
         << rejected << "\n";

    cout << "Wall time (seconds)  : "
         << wall_seconds << "\n";

    cout << "Throughput (req/s)   : "
         << throughput << "\n";

    cout << "\n";
    cout << "Latency (microseconds)\n";

    cout << "p50                  : "
         << percentile(latencies, 0.50) << "\n";

    cout << "p95                  : "
         << percentile(latencies, 0.95) << "\n";

    cout << "p99                  : "
         << percentile(latencies, 0.99) << "\n";

    cout << "max                  : "
         << percentile(latencies, 1.00) << "\n";

    cout << "\n";
    cout << "Worker metrics\n";

    cout << "Busy time (seconds)  : "
         << busy_seconds << "\n";

    cout << "Idle time (seconds)  : "
         << idle_seconds << "\n";

    cout << "Worker utilization   : "
         << utilization << "%\n";

    cout << "Max queue depth      : "
         << max_depth << "\n";

    cout << "========================================\n";
}

/*
    Benchmark TokenBucket.

    Important:
        We generate the exact same arrival schedule
        used by QueueProcess.

    The sleep is in the traffic generator, not
    inside the rate limiter.
*/
void benchmarkTokenBucket(
    const vector<uint64_t>& arrivals,
    unsigned int capacity,
    unsigned int refill_rate
) {
    cout << "\nStarting TokenBucket benchmark...\n";

    Metrics metrics;

    TokenBucket bucket(capacity, refill_rate);

    vector<uint64_t> latencies;
    latencies.reserve(TOTAL_REQUESTS);

    TimePoint benchmark_start = Clock::now();

    TimePoint next_arrival = benchmark_start;

    for (uint64_t i = 0; i < TOTAL_REQUESTS; ++i) {
        next_arrival =
            benchmark_start +
            chrono::nanoseconds(arrivals[i]);

        /*
            Real-time traffic generation.

            No sleep inside TokenBucket.
        */
        this_thread::sleep_until(next_arrival);

        TimePoint request_start = Clock::now();

        bool allowed = bucket.allowRequest();

        TimePoint request_end = Clock::now();

        uint64_t latency_ns =
            nsBetween(request_start, request_end);

        RequestResult result;
        result.accepted = allowed;
        result.latency_ns = latency_ns;

        metrics.record(result);

        latencies.push_back(latency_ns);
    }

    TimePoint benchmark_end = Clock::now();

    double wall_seconds =
        static_cast<double>(
            chrono::duration_cast<chrono::nanoseconds>(
                benchmark_end - benchmark_start
            ).count()
        ) / 1000000000.0;

    printReport(
        "TokenBucket",
        metrics,
        latencies,
        wall_seconds
    );
}

/*
    Benchmark QueueProcess.

    Every request is placed into the queue.

    The promise/future is used to measure the time
    until the worker finishes the request.

    The callback itself performs no artificial sleep.
*/
void benchmarkQueueProcess(
    const vector<uint64_t>& arrivals
) {
    cout << "\nStarting QueueProcess benchmark...\n";

    Metrics metrics;

    QueueProcess qp(&metrics);

    thread worker(
        &QueueProcess::processor,
        &qp
    );

    vector<uint64_t> latencies;
    latencies.reserve(TOTAL_REQUESTS);

    TimePoint benchmark_start = Clock::now();

    TimePoint next_arrival = benchmark_start;

    for (uint64_t i = 0; i < TOTAL_REQUESTS; ++i) {
        next_arrival =
            benchmark_start +
            chrono::nanoseconds(arrivals[i]);

        /*
            Traffic generator delay only.
            No artificial delay in the worker.
        */
        this_thread::sleep_until(next_arrival);

        TimePoint request_start = Clock::now();

        shared_ptr<promise<RequestResult>> prom =
            make_shared<promise<RequestResult>>();

        future<RequestResult> ft =
            prom->get_future();

        qp.newRequest(
            [prom, request_start]() mutable {
                TimePoint request_end = Clock::now();

                RequestResult result;
                result.accepted = true;
                result.latency_ns =
                    nsBetween(request_start, request_end);

                prom->set_value(result);
            }
        );

        RequestResult result = ft.get();

        metrics.record(result);

        latencies.push_back(result.latency_ns);
    }

    /*
        Stop after all requests have completed.

        The worker drains any remaining work,
        then exits.
    */
    qp.stop();

    worker.join();

    TimePoint benchmark_end = Clock::now();

    double wall_seconds =
        static_cast<double>(
            chrono::duration_cast<chrono::nanoseconds>(
                benchmark_end - benchmark_start
            ).count()
        ) / 1000000000.0;

    printReport(
        "QueueProcess",
        metrics,
        latencies,
        wall_seconds
    );
}

int main(int argc, char** argv) {
    double arrival_rate = 100000.0;

    unsigned int capacity = 10;
    unsigned int refill_rate = 1;

    uint64_t seed = 42;

    if (argc >= 2) {
        arrival_rate = atof(argv[1]);
    }

    if (argc >= 3) {
        capacity = static_cast<unsigned int>(
            atoi(argv[2])
        );
    }

    if (argc >= 4) {
        refill_rate = static_cast<unsigned int>(
            atoi(argv[3])
        );
    }

    if (arrival_rate <= 0.0) {
        cerr << "Arrival rate must be > 0\n";
        return 1;
    }

    cout << "========================================\n";
    cout << "Rate Limiter Benchmark\n";
    cout << "========================================\n";

    cout << "Requests              : "
         << TOTAL_REQUESTS << "\n";

    cout << "Arrival rate          : "
         << arrival_rate << " req/s\n";

    cout << "Token capacity        : "
         << capacity << "\n";

    cout << "Token refill rate     : "
         << refill_rate << " tokens/s\n";

    cout << "Random seed           : "
         << seed << "\n";

    cout << "========================================\n";

    cout << "\nGenerating arrival schedule...\n";

    vector<uint64_t> arrivals =
        generateArrivals(
            TOTAL_REQUESTS,
            arrival_rate,
            seed
        );

    cout << "Arrival schedule ready.\n";

    /*
        Both tests use the same arrivals.

        Each test is run independently.
    */
    benchmarkTokenBucket(
        arrivals,
        capacity,
        refill_rate
    );

    benchmarkQueueProcess(
        arrivals
    );

    return 0;
}