#include <iostream>
#include <map>
#include <iomanip>
#include <vector>
#include <algorithm>
#include <random>
#include <iostream>
#include <vector>
#include <chrono>
#include <iomanip>
#include <ctime>

using namespace std;

#define KEYS 100000
#define TXNS 100000

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

int main (){
	std::map<int,int> data;
    vector<int> zipf_dist;
	int i;
	timespec start, finish;

    for(i = 0; i<KEYS ; i++)
    	data[i] = i;

    // LOW SKEW
    zipf_dist = zipf(0.1, KEYS, TXNS);

    int val;
    int key;

    return 0;
}
