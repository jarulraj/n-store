
#include <algorithm>
#include <random>
#include <iostream>
#include <vector>

#include "utils.h"

using namespace std;

// RANDOM DIST

void random_string(char* str, size_t len ){
	static const char alphanum[] = "0123456789"
			"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
			"abcdefghijklmnopqrstuvwxyz";

	for (int i = 0; i < len; ++i) {
		str[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
	}

	str[len] = 0;
}

// Get a value in [0, N-1] that follows zipf distribution
// P(i) = C/I^Alpha for i = 1 to N
vector<int> zipf(double alpha, int n, int num_values) {
	static double c = 0;          // Normalization constant
	double z;                     // Uniform random number (0 < z < 1)
	double sum_prob;              // Sum of probabilities
	double zipf_value;            // Computed exponential value to be returned
	int i, j;

	vector<int> zipf_dist;
	double* powers = new double[n+1];
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(0, 1);

	for (i = 1; i <= n; i++)
		powers[i] = 1.0 / pow((double) i, alpha);

	// Compute normalization constant on first call only
	for (i = 1; i <= n; i++)
		c = c + powers[i];
	c = 1.0 / c;

	for (j = 1; j <= num_values; j++) {
		// Pull a uniform random number in (0,1)
		do { z = dis(gen); } while ((z == 0) || (z == 1));

		// Map z to the value
		sum_prob = 0;
		for (i = 1; i <= n; i++) {
			sum_prob = sum_prob + c * powers[i];
			if (sum_prob >= z) {
				zipf_value = i;
				break;
			}
		}

		zipf_dist.push_back(zipf_value);
	}

	delete powers;

	return zipf_dist;
}

// PTHREAD WRAPPERS

void wrlock(pthread_rwlock_t* access){
	int rc = -1;
	rc = pthread_rwlock_wrlock(access);

	if(rc != 0){
		printf("wrlock failed \n");
		exit(EXIT_FAILURE);
	}
}

void rdlock(pthread_rwlock_t* access){
	int rc = -1;
	rc = pthread_rwlock_rdlock(access);

	if(rc != 0){
		printf("rdlock failed \n");
		exit(EXIT_FAILURE);
	}
}

void unlock(pthread_rwlock_t* access){
	int rc = -1;
	rc = pthread_rwlock_unlock(access);

	if(rc != 0){
		printf("unlock failed \n");
		exit(EXIT_FAILURE);
	}
}

