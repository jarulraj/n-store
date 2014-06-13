#ifndef YCSB_BENCHMARK_H_
#define YCSB_BENCHMARK_H_

#include <cstdio>
#include <cstring>
#include <vector>

#include "benchmark.h"
#include "key.h"
#include "record.h"
#include "statement.h"
#include "utils.h"
#include "nstore.h"

using namespace std;

class ycsb_benchmark : public benchmark {
 public:
  ycsb_benchmark();
  ycsb_benchmark(config& _conf);

  workload* get_dataset();
  workload* get_workload();

 private:
  vector<int> zipf_dist;
  vector<double> uniform_dist;

  workload load;
  config& conf;

};

#endif /* YCSB_BENCHMARK_H_ */
