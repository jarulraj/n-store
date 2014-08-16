#ifndef YCSB_BENCHMARK_H_
#define YCSB_BENCHMARK_H_

#include <cstdio>
#include <cstring>
#include <vector>

#include "benchmark.h"
#include "field.h"
#include "record.h"
#include "statement.h"
#include "utils.h"
#include "nstore.h"
#include "status.h"
#include "libpm.h"
#include "plist.h"
#include "serializer.h"

using namespace std;

class ycsb_benchmark : public benchmark {
 public:
  ycsb_benchmark(config _conf, unsigned int tid, database* _db, timer* _tm);

  void load();
  void execute();
  void reset();

  void sim_crash();

  void do_update(engine* ee);
  void do_read(engine* ee);

  // Table Ids
  const int USER_TABLE_ID = 0;

  // Schema
  schema* user_table_schema;

  vector<int> zipf_dist;
  vector<double> uniform_dist;

  config conf;
  vector<int> update_field_ids;
  serializer sr;

  benchmark_type btype;

  unsigned int txn_id;
  unsigned int num_keys;
  unsigned int num_txns;

};

#endif /* YCSB_BENCHMARK_H_ */
