#ifndef _UTILS_H_
#define _UTILS_H_

#include <vector>
#include <ctime>

using namespace std;

// UTILS

void random_string(char* str, size_t len );

vector<int> zipf(double alpha, int n, int num_values);

void display_stats(timespec start, timespec finish, int num_txns);

void wrlock(pthread_rwlock_t* access);

void unlock(pthread_rwlock_t* access);

void rdlock(pthread_rwlock_t* access);

#endif /* UTILS_H_ */
