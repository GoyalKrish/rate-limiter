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

#include "crow.h"
#include "RateLimiter.h"

long long percentile(vector<long long> sorted_samples, double p){
    int n = sorted_samples.size();
    if(n == 0) return 0;

    if(p < 0.0 || p > 1.0) throw invalid_argument("p must be in the range : (0,1]");
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

    std::cout << "total : " << total << endl << "accepted : " << accepted << "\nrejected : " << total-accepted << endl;

    
}

int main(){
    TokenBucket tb(100,10);

    vector<long long> samples;
    unsigned long accepted;
    int total = 100;


    auto start = chrono::steady_clock::now();
    thread t1(&runReq, ref(tb), total, ref(samples), ref(accepted));
    t1.join();
    auto end = chrono::steady_clock::now();

    auto time_taken = chrono::duration_cast<chrono::nanoseconds>(end - start).count();

    vector<long long> sorted_samples = samples;
    sort(sorted_samples.begin(), sorted_samples.end());

    // cout << "samples are : ";
    // for(const long long& s : sorted_samples) cout << s << "\t";
    // cout << endl;

    long long p50 = percentile(sorted_samples,0.50);
    long long p90 = percentile(sorted_samples,0.90);
    long long p99 = percentile(sorted_samples,0.99);

    cout << "p50 = " << p50 << " p90 = " << p90 << " p99 = " << p99 << " total accepted = " << accepted << endl;
    cout << " time taken for thread = " << time_taken << "ns throughput = " << total/(time_taken/1e9); 




}