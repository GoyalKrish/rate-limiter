#include<thread>
#include<chrono>
#include<iostream>
using namespace std;
long long started_at;
void worker(int id){
    for(int i = 0 ; i < 5 ; ++i){
        cout << endl << "worker " << id << " doing work " << i << " at " << to_string(chrono::duration_cast<chrono::milliseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count() - started_at);
        
        this_thread::sleep_for(chrono::milliseconds(100)); 
    }
}

int main(){
    started_at = chrono::duration_cast<chrono::milliseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
    thread t1(worker, 10);
    thread t2(worker, 20);

    t1.join();
    t2.join();

    return 0;
}