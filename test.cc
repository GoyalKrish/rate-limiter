#include "crow.h"
#include "RateLimiter.h"

// crow::response hello(){
//     return crow::response(200, "hello user " + to_string(nowTime()));
// }

// int main(){
//     TokenBucket rh(10,1);

//     crow::SimpleApp app;

    

//     CROW_ROUTE(app, "/")([&rh]() {

//         if (rh.allowRequest()) {
//             return crow::response(200, "Request allowed at " + to_string(nowTime()));
//         }

//         return crow::response(429, "Too many requests");
//     });

//     app.port(8080).multithreaded().run();
// }




int main(){
    QueueProcess qp(10);

    thread worker(&QueueProcess::processor, &qp);

    crow::SimpleApp app;

    

    CROW_ROUTE(app, "/hello")([&qp]() {
        shared_ptr<promise<string>> prom = make_shared<promise<string>>();
        future<string> ft = prom->get_future();
        
        qp.newRequest([prom = move(prom)]() mutable{
            prom->set_value("hello at " + to_string(nowTime()));
        });

        string res = ft.get();
        return crow::response(200,res);
    
    });

    app.port(8080).multithreaded().run();

    worker.join();
}
