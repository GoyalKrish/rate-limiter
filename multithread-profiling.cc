#include "crow.h"
#include "RateLimiter.h"

class Results{
  public:
    vector<long long> samples;
    unsigned long accepted = 0;
};

long long percentile(const vector<long long>& sorted_samples, double p){
    int n = sorted_samples.size();
    if(n == 0) return 0;

    if(p <= 0.0 || p > 1.0) throw invalid_argument("p must be in the range : (0,1]");
    // we'll assume that we're getting sorted samples as it would be a computational waste to sort them again and again;
    // sort(samples.begin(), samples.end());
    
    return sorted_samples[ceil(p * n) - 1]; 
}


void runReq(TokenBucket& tb,const int total, vector<long long> &samples, unsigned long &accepted){
    
    samples.resize(total);
    accepted = 0;

    for(int i = 0 ; i < total ; ++i){
        auto start = chrono::steady_clock::now();
        bool resp = tb.allowRequest();
        auto end = chrono::steady_clock::now();

        auto latency = chrono::duration_cast<chrono::nanoseconds>(end - start).count();

        if(resp) ++accepted;
        samples[i] = latency;
    }

    // std::cout << "total : " << total << endl << "accepted : " << accepted << "\nrejected : " << total-accepted << endl;

    
}


int main(int argc, char* argv[]){
    TokenBucket tb(100,10);
    
    if(argc != 3) throw invalid_argument("provide thread_count and requests_per_thread");

    const int thread_count = stoi(argv[1]);
    const long requests_per_thread = stoi(argv[2]);
    
    vector<thread> threads;
    vector<Results> res(thread_count);

    auto start = chrono::steady_clock::now();

    for(int i = 0 ; i < thread_count ; ++i){
        threads.emplace_back(&runReq, ref(tb), requests_per_thread, ref(res[i].samples), ref(res[i].accepted));
    }

    for(auto& t : threads) t.join();

    auto end = chrono::steady_clock::now();

    auto time_taken = chrono::duration_cast<chrono::nanoseconds>(end - start).count();


    // calculate total results :
    long long total_requests = thread_count * requests_per_thread;
    int total_accepted_requests = 0;
    vector<long long> all_samples;
    double time_taken_in_seconds = time_taken / 1e9;

    
    for(const auto& r : res){
        total_accepted_requests += r.accepted;
        
        all_samples.insert(all_samples.end(),r.samples.begin(), r.samples.end());
    }
    
    long long total_rejected_requests = total_requests - total_accepted_requests;
    sort(all_samples.begin(),all_samples.end());
    double p50 = percentile(all_samples,0.50) / 1000.0;
    double p90 = percentile(all_samples,0.90) / 1000.0;
    double p99 = percentile(all_samples,0.99) / 1000.0;

    double throughput = total_requests/time_taken_in_seconds;

    cout << fixed << setprecision(3);
    cout << "========== Concurrent Rate Limiter Benchmark ==========" << endl;
    cout << "Threads : " << thread_count << endl;
    cout << "Requests per thread : " << requests_per_thread << endl;
    cout << "Total requests : " << total_requests << endl;
    cout << endl;
    cout << "Accepted requests : " << total_accepted_requests << endl;
    cout << "Rejected requests : " << total_rejected_requests << endl;
    cout << endl;
    cout << "Wall Time : " << time_taken_in_seconds << " seconds" << endl;
    cout << "Throughput : " << throughput << " requests/sec" << endl;
    cout << endl;
    cout << "latency :" << endl;
    cout << "p50 : " << p50 << " us" <<endl;
    cout << "p90 : " << p90 << " us" <<endl;
    cout << "p99 : " << p99 << " us" <<endl;
    cout << "=======================================================" << endl;




}
