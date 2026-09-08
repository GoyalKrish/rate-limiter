    #include<chrono>
    #include<algorithm>
    #include<mutex>
    using namespace std;

    long long nowTime(){
        return chrono::duration_cast<chrono::milliseconds>(chrono::steady_clock::now().time_since_epoch()).count();
    }

    class RateLimiter{
        double tokens;
        unsigned int capacity;
        unsigned int refill_rate;
        long long last_time;
        mutex mtx;

        public:
            RateLimiter(int cap, int rate){
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