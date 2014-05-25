#ifndef _UTILS_H_
#define _UTILS_H_

#include <vector>

using namespace std;

// UTILS

void random_string(char* str, size_t len );

vector<int> zipf(double alpha, int n, int num_values);

void wrlock(pthread_rwlock_t* access);

void unlock(pthread_rwlock_t* access);

void rdlock(pthread_rwlock_t* access);

#endif /* UTILS_H_ */
