    #include<chrono>
    #include<algorithm>
    using namespace std;

    long long nowTime(){
        return chrono::duration_cast<chrono::milliseconds>(chrono::steady_clock::now().time_since_epoch()).count();
    }

    class RateLimiter{
        double tokens;
        unsigned int capacity;
        unsigned int refill_rate;
        long long last_time;

        public:
            RateLimiter(int cap, int rate){
                tokens = cap;
                capacity = cap;
                refill_rate = rate;
                last_time = nowTime();
            }


            bool allowRequest(){
            // 1. Check how much time passed since last_time
            long long now_time = nowTime();
            long long diff = now_time - last_time;
            last_time = now_time;
            // 2. Calculate how many credits to add based on time passed
            double canAdd = (diff / 1000.0) * refill_rate;
            // 3. Add credits (but don't exceed capacity)
            tokens = min((double)capacity, tokens + canAdd);
            // 4. If tokens >= 1, use 1 token and return true
            if(tokens >= 1){
                --tokens;
                return true;
            }else return false;
            // 5. Otherwise return false

            }
    };