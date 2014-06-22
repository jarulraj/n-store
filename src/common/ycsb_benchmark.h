#ifndef YCSB_BENCHMARK_H_
#define YCSB_BENCHMARK_H_

#include <cstdio>
#include <cstring>
#include <vector>

#include "nstore.h"
#include "benchmark.h"

using namespace std;

class ycsb_benchmark : public benchmark {
 public:
  ycsb_benchmark();
  ycsb_benchmark(config& _conf);

  workload& get_dataset();
  workload& get_workload();

  vector<int> zipf_dist;
  vector<double> uniform_dist;

  workload load;
  config& conf;

  unsigned int s_id;
};

#endif /* YCSB_BENCHMARK_H_ */
