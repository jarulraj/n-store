#ifndef YCSB_BENCHMARK_H_
#define YCSB_BENCHMARK_H_

#include <cstdio>
#include <cstring>
#include <vector>

#include "nstore.h"
#include "benchmark.h"
#include "database.h"
#include "engine.h"
#include "timer.h"

using namespace std;

class ycsb_benchmark : public benchmark {
 public:
  ycsb_benchmark(config& _conf);

  void load(engine* ee);
  void execute(engine* ee);
  void execute_one(engine* ee);

  void do_update(engine* ee, unsigned int txn_itr, schema* usertable_schema, const vector<int>& field_ids);
  void do_read(engine* ee, unsigned int txn_itr, schema* usertable_schema);

  vector<int> simple_dist;
  vector<double> uniform_dist;

  config& conf;
  benchmark_type btype;

  unsigned int txn_id;
  timer tm;
};

#endif /* YCSB_BENCHMARK_H_ */
