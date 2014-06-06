#ifndef _UTILS_H_
#define _UTILS_H_

#include <vector>
#include <ctime>

using namespace std;

// UTILS

inline void random_string(char* str, size_t len ){
	static const char alphanum[] = "0123456789"
			"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
			"abcdefghijklmnopqrstuvwxyz";

	char rep = alphanum[rand() % (sizeof(alphanum) - 1)];
	for (int i = 0; i < len; ++i)
		str[i] = rep;
	str[len-1] = '\0';
}

void simple_skew(vector<int>& zipf_dist, int n, int num_values);

void zipf(vector<int>& zipf_dist, double alpha, int n, int num_values);

void uniform(vector<double>& uniform_dist, int num_values);

void display_stats(timespec start, timespec finish, int num_txns);

void wrlock(pthread_rwlock_t* access);

void unlock(pthread_rwlock_t* access);

void rdlock(pthread_rwlock_t* access);

#endif /* UTILS_H_ */
