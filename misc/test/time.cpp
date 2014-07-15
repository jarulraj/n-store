#include <iostream>
#include <chrono>
#include <iomanip>

#include <sys/time.h>
using namespace std;

long fibonacci(int n) {
  if (n < 3)
    return 1;
  return fibonacci(n - 1) + fibonacci(n - 2);
}

int main() {
  std::chrono::time_point<std::chrono::system_clock> start, end;
  start = std::chrono::system_clock::now();
  std::cout << "f(42) = " << fibonacci(40) << '\n';
  end = std::chrono::system_clock::now();

  std::chrono::duration<double> elapsed_seconds = end - start;

  std::cout << "elapsed time: " << elapsed_seconds.count() << "s\n";

  std::chrono::time_point<std::chrono::high_resolution_clock> hr_start, hr_end;
  hr_start = std::chrono::high_resolution_clock::now();
  std::cout << "f(42) = " << fibonacci(40) << '\n';
  hr_end = std::chrono::high_resolution_clock::now();

  std::chrono::duration<double> hr_elapsed_seconds = hr_end - hr_start;

  std::cout << "hr_elapsed time: " << hr_elapsed_seconds.count() << "s\n";

  timeval t1, t2;

  gettimeofday(&t1, NULL);
  std::cout << "f(42) = " << fibonacci(40) << '\n';
  gettimeofday(&t2, NULL);

  double duration;

  duration = (t2.tv_sec - t1.tv_sec) * 1000.0;      // sec to ms
  duration += (t2.tv_usec - t1.tv_usec) / 1000.0;   // us to ms

  cout << std::fixed << std::setprecision(2);
  cout << "Duration (s) : " << (duration / 1000.0) << "\n";

  return 0;

}
