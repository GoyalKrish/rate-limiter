    #include<chrono>
    #include<algorithm>
    #include<mutex>
    #include<queue>
    using namespace std;

    long long nowTime(){
        return chrono::duration_cast<chrono::milliseconds>(chrono::steady_clock::now().time_since_epoch()).count();
    }

    class TokenBucket{
        double tokens;
        unsigned int capacity;
        unsigned int refill_rate;
        long long last_time;
        mutex mtx;

        public:
            TokenBucket(int cap, int rate){
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
                
                double canAdd = (diff / 1000.0) * refill_rate;
                tokens = min((double)capacity, tokens + canAdd);

                if(tokens >= 1){
                    --tokens;
                    return true;
                }else return false;
            }
    };


    class QueueProcess{
        queue<function<void()>> q;
        long long waiting_time_ms;
        mutex mtx;
        condition_variable cv;

        bool stopped = false;

        public:
            QueueProcess(long long req_per_sec){
                waiting_time_ms = 1000 / req_per_sec;
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

                    fn();

                    this_thread::sleep_for(chrono::milliseconds(waiting_time_ms));
                }
            }


            void newRequest(function<void()> fn){
                {
                    lock_guard<mutex> lock(mtx);
                    q.push(fn);
                }

                cv.notify_one();
            }
    };
