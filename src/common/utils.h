#ifndef _UTILS_H_
#define _UTILS_H_

#include <vector>

using namespace std;

typedef std::chrono::time_point<std::chrono::high_resolution_clock> tpoint;

// UTILS

void random_string(char* str, size_t len );

vector<int> zipf(double alpha, int n, int num_values);

void display_stats(tpoint start, tpoint finish, int num_txns);

void wrlock(pthread_rwlock_t* access);

void unlock(pthread_rwlock_t* access);

void rdlock(pthread_rwlock_t* access);

#endif /* UTILS_H_ */
