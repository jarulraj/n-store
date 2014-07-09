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

void display_stats(timeval t1, timeval t2, int num_txns) {
  double duration;
  double throughput;

  duration = (t2.tv_sec - t1.tv_sec) * 1000.0;      // sec to ms
  duration += (t2.tv_usec - t1.tv_usec) / 1000.0;   // us to ms

  cout << std::fixed << std::setprecision(2);
  cout << "Duration(s) : " << (duration / 1000.0) << " ";

  throughput = (num_txns * 1000.0) / duration;
  cout << "Throughput  : " << throughput << endl;
}

// RANDOM DIST
void zipf(vector<int>& zipf_dist, double alpha, int n, int num_values) {
  static double c = 0;          // Normalization constant
  double z;                     // Uniform random number (0 < z < 1)
  double sum_prob;              // Sum of probabilities
  double zipf_value;            // Computed exponential value to be returned
  int i, j;

  double* powers = new double[n + 1];
  int seed = 0;
  std::mt19937 gen(seed);
  std::uniform_real_distribution<> dis(0.001, 0.099);
  int avg = n/2;
 
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

    zipf_value = ((int)zipf_value + avg)%n;
    zipf_dist.push_back(zipf_value);
  }

  delete[] powers;
}

// Simple skew generator
void simple_skew(vector<int>& simple_dist, int n, int num_values) {
  int seed = 0;
  std::mt19937 gen(seed);
  std::uniform_real_distribution<> dis(0, 1);
  double i, z;
  long bound = n / 10;  // 90% from 10% of values
  long val, diff;

  diff = n - bound;

  for (i = 0; i < num_values; i++) {
    z = dis(gen);

    if (z < 0.9)
      val = z * bound;
    else
      val = bound + z * diff;

    simple_dist.push_back(val);
  }
}

void uniform(vector<double>& uniform_dist, int num_values) {
  int seed = 0;
  std::mt19937 gen(seed);
  std::uniform_real_distribution<> dis(0, 1);
  double i;

  for (i = 0; i < num_values; i++)
    uniform_dist.push_back(dis(gen));
}

// PTHREAD WRAPPERS

void wrlock(pthread_rwlock_t* access) {
  int rc = pthread_rwlock_wrlock(access);

  if (rc != 0) {
    printf("wrlock failed \n");
    exit(EXIT_FAILURE);
  }
}

void rdlock(pthread_rwlock_t* access) {
  int rc = pthread_rwlock_rdlock(access);

  if (rc != 0) {
    printf("rdlock failed \n");
    exit(EXIT_FAILURE);
  }
}

void unlock(pthread_rwlock_t* access) {
  int rc = pthread_rwlock_unlock(access);

  if (rc != 0) {
    printf("unlock failed \n");
    exit(EXIT_FAILURE);
  }
}

