#ifndef TPCC_BENCHMARK_H_
#define TPCC_BENCHMARK_H_

#include <cstdio>
#include <cstring>
#include <vector>

#include "nstore.h"
#include "statement.h"
#include "utils.h"
#include "status.h"
#include "benchmark.h"
#include "database.h"
#include "engine.h"
#include "timer.h"

using namespace std;

class tpcc_benchmark : public benchmark {
 public:
  tpcc_benchmark(config& _conf);

  void load(engine* ee);
  void execute(engine* ee);

  void do_read(engine* ee, unsigned int txn_itr, schema* usertable_schema);

  vector<double> uniform_dist;

  config& conf;
  benchmark_type btype;

  unsigned int txn_id;
  timer tm;
};

#endif /* TPCC_BENCHMARK_H_ */
