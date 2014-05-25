#include <iostream>
#include <chrono>
#include <ctime>

long fibonacci(int n)
{
    if (n < 3) return 1;
    return fibonacci(n-1) + fibonacci(n-2);
}

int main()
{
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
}
