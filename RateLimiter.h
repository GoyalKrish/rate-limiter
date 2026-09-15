#include <chrono>
#include <algorithm>
#include <mutex>
#include <queue>
#include <functional>
#include <condition_variable>
#include <thread>
#include <future>
#include <memory>

using namespace std;
    long long nowTime(){
        return chrono::duration_cast<chrono::nanoseconds>(chrono::steady_clock::now().time_since_epoch()).count();
    }

    class TokenBucket{
        double tokens;
        unsigned int capacity;
        unsigned int refill_rate;
        long long last_time;
        mutex mtx;

        public:
            TokenBucket(unsigned int cap,unsigned int rate){
                tokens = cap;
                capacity = cap;
                refill_rate = rate;
                last_time = nowTime();
            }


            bool allowRequest(){
                lock_guard<mutex> lock(mtx);
                long long now_time = nowTime();
                long long diff = now_time - last_time;
                last_time = now_time;
                
                double canAdd = (diff / 1e9) * refill_rate;
                tokens = min((double)capacity, tokens + canAdd);

                if(tokens >= 1){
                    --tokens;
                    return true;
                }else return false;
            }
    };


    class QueueProcess{
        public:
        queue<function<void()>> q;
        long long waiting_time_ns;
        size_t max_queue_size;
        chrono::steady_clock::time_point next_exec_time;

        mutex mtx;
        condition_variable cv;

        bool stopped = false;

            QueueProcess(long long req_per_sec, size_t max_size){
                if(req_per_sec == 0)
                    waiting_time_ns = 0;
                else
                    waiting_time_ns = 1e9 / req_per_sec;

                max_queue_size = max_size;
                next_exec_time = chrono::steady_clock::now();

            }

            void processor(){
                while(true){
                    function<void()> fn;
                    {
                        unique_lock<mutex> lock(mtx);
                        cv.wait(lock,[&](){
                            return !q.empty() || stopped;
                        });

                        if(stopped && q.empty()) return;

                        fn = q.front();
                        q.pop();
                    }
                    chrono::steady_clock::time_point now = chrono::steady_clock::now();
                    if(now < next_exec_time) this_thread::sleep_until(next_exec_time); 
                    fn();
                    next_exec_time += chrono::nanoseconds(waiting_time_ns);
                    now = chrono::steady_clock::now();
                    if(next_exec_time < now) next_exec_time = now;
                }
            }


            bool newRequest(function<void()> fn){
                {
                    lock_guard<mutex> lock(mtx);
                    if(stopped) return false;
                    if(q.size() >= max_queue_size) return false;
                    q.push(fn);
                }

                cv.notify_one();
                return true;
            }

            void stop(){
                {
                    lock_guard<mutex> lock(mtx);
                    stopped = true;
                }
                cv.notify_all();
            }
    };

