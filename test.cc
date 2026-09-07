#include "crow.h"
#include "RateLimiter.h"

int main(){
    RateLimiter rh(10,1);

    crow::SimpleApp app;

    

    CROW_ROUTE(app, "/")([&rh]() {

        if (rh.allowRequest()) {
            return crow::response(200, "Request allowed at " + to_string(nowTime()));
        }

        return crow::response(429, "Too many requests");
    });

    app.port(8080).multithreaded().run();
}

