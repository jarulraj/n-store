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
  void handler(engine* ee, unsigned int tid);

  void sim_crash(engine* ee);

  void do_update(engine* ee, unsigned int tid);
  void do_read(engine* ee, unsigned int tid);

  // Table Ids
  const int USER_TABLE_ID = 0;

  // Schema
  schema* user_table_schema;

  vector<int> simple_dist;
  vector<double> uniform_dist;

  config& conf;
  vector<int> update_field_ids;

  benchmark_type btype;

  unsigned int txn_id;
};

#endif /* YCSB_BENCHMARK_H_ */
