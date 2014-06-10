
#include <algorithm>
#include <random>
#include <iostream>
#include <vector>
#include <chrono>
#include <iomanip>
#include <ctime>
#include <cassert>

#include "utils.h"

using namespace std;

// TIMER

timespec diff(timespec start, timespec finish){
	timespec temp;
	if ((finish.tv_nsec-start.tv_nsec)<0) {
		temp.tv_sec = finish.tv_sec-start.tv_sec-1;
		temp.tv_nsec = 1000000000+finish.tv_nsec-start.tv_nsec;
	} else {
		temp.tv_sec = finish.tv_sec-start.tv_sec;
		temp.tv_nsec = finish.tv_nsec-start.tv_nsec;
	}

	return temp;
}

void display_stats(timespec start, timespec finish, int num_txns){
    timespec elapsed_seconds;
    double duration;
    double throughput;

    elapsed_seconds = diff(start, finish);
    duration = elapsed_seconds.tv_sec + (double)(elapsed_seconds.tv_nsec)/(double)100000000;
    cout <<std::fixed << std::setprecision(2) ;

    //cout<< elapsed_seconds.tv_sec <<":"<< elapsed_seconds.tv_nsec <<endl;
	cout << "Duration(s) : " << duration << " ";

	throughput = (double)num_txns/(double)duration;
	cout << "Throughput  : " << throughput << endl;
}


// RANDOM DIST


// Get a value in [0, N-1] that follows zipf distribution
// P(i) = C/I^Alpha for i = 1 to N
void zipf(vector<int>& zipf_dist, double alpha, int n, int num_values) {
	static double c = 0;          // Normalization constant
	double z;                     // Uniform random number (0 < z < 1)
	double sum_prob;              // Sum of probabilities
	double zipf_value;            // Computed exponential value to be returned
	int i, j;

	double* powers = new double[n+1];
    int seed = 0;
    std::mt19937 gen(seed);
    std::uniform_real_distribution<> dis(0.001, 0.099);

	for (i = 1; i <= n; i++)
		powers[i] = 1.0 / pow((double) i, alpha);

	// Compute normalization constant on first call only
	for (i = 1; i <= n; i++)
		c = c + powers[i];
	c = 1.0 / c;

	for (i = 1; i <= n; i++)
			powers[i] = c * powers[i];

	for (j = 1; j <= num_values; j++) {

		// Pull a uniform random number in (0,1)
		z = dis(gen);

		// Map z to the value
		sum_prob = 0;
		for (i = 1; i <= n; i++) {
			sum_prob = sum_prob + powers[i];
			if (sum_prob >= z) {
				zipf_value = i;
				break;
			}
		}

		zipf_dist.push_back(zipf_value);
	}

	delete powers;
}

// Simple skew generator
void simple_skew(vector<int>& zipf_dist, int n, int num_values) {
    int seed = 0;
    std::mt19937 gen(seed);
    std::uniform_real_distribution<> dis(0, 1);
    double i, z;
    long bound = n/10; // 90% from 10% of values
    long val, diff;

    diff = n - bound;

	for (i = 0; i < num_values; i++){
		z = dis(gen);

		if(z < 0.9)
			val = z * bound;
		else
			val = bound + z * diff;

		zipf_dist.push_back(val);
	}
}

void uniform(vector<double>& uniform_dist, int num_values) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(0, 1);
    double i, z;

	for (i = 0; i < num_values; i++)
		uniform_dist.push_back(dis(gen));
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

