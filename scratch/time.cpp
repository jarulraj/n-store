#include <iostream>
#include <chrono>
#include <ctime>

#include <iostream>
#include <time.h>
using namespace std;

timespec diff(timespec start, timespec end){
	timespec temp;
	if ((end.tv_nsec-start.tv_nsec)<0) {
		temp.tv_sec = end.tv_sec-start.tv_sec-1;
		temp.tv_nsec = 1000000000+end.tv_nsec-start.tv_nsec;
	} else {
		temp.tv_sec = end.tv_sec-start.tv_sec;
		temp.tv_nsec = end.tv_nsec-start.tv_nsec;
	}
	return temp;
}

long fibonacci(int n){
    if (n < 3) return 1;
    return fibonacci(n-1) + fibonacci(n-2);
}

int main(){
    std::chrono::time_point<std::chrono::system_clock> start, end;
    start = std::chrono::system_clock::now();
    std::cout << "f(42) = " << fibonacci(42) << '\n';
    end = std::chrono::system_clock::now();

    std::chrono::duration<double> elapsed_seconds = end-start;

    std::cout << "elapsed time: " << elapsed_seconds.count() << "s\n";

    std::chrono::time_point<std::chrono::high_resolution_clock> hr_start, hr_end;
    hr_start = std::chrono::high_resolution_clock::now();
    std::cout << "f(42) = " << fibonacci(42) << '\n';
    hr_end = std::chrono::high_resolution_clock::now();

    std::chrono::duration<double> hr_elapsed_seconds = hr_end-hr_start;

    std::cout << "hr_elapsed time: " << hr_elapsed_seconds.count() << "s\n";

    timespec time1, time2;
	int temp;

	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &time1);
    std::cout << "f(42) = " << fibonacci(42) << '\n';
	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &time2);

	std::cout << diff(time1, time2).tv_sec << ":" << diff(time1, time2).tv_nsec << endl;
	return 0;

}
