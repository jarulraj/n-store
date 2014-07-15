// Zipf distribution

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cmath>
#include <random>
#include <iostream>
#include <vector>

using namespace std;

// Get a value in [1, N] that follows zipf distribution
// P(i) = C/I^Alpha for i = 1 to N
vector<int> zipf(double alpha, int n, int num_values) {
  static double c = 0;          // Normalization constant
  double z;                     // Uniform random number (0 < z < 1)
  double sum_prob;              // Sum of probabilities
  double zipf_value;            // Computed exponential value to be returned
  int i, j;

  vector<int> zipf_dist;
  double* powers = new double[n + 1];
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
    do {
      z = dis(gen);
    } while ((z == 0) || (z == 1));

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

  delete[] powers;

  return zipf_dist;
}

int main(void) {
  double alpha = 1.0;
  double n = 10;
  int num_values = 100000;

  zipf(alpha, n, num_values);

  return 0;
}
