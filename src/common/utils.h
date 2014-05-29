#ifndef _UTILS_H_
#define _UTILS_H_

#include <vector>
#include <ctime>

using namespace std;

// UTILS

void random_string(char* str, size_t len );

void zipf(vector<int>& zipf_dist, double alpha, int n, int num_values);

void uniform(vector<double>& uniform_dist, int num_values);

void display_stats(timespec start, timespec finish, int num_txns);

void wrlock(pthread_rwlock_t* access);

void unlock(pthread_rwlock_t* access);

void rdlock(pthread_rwlock_t* access);

#endif /* UTILS_H_ */
