#pragma once

#include <cstdio>
#include <cstring>
#include <vector>

#include "benchmark.h"
#include "field.h"
#include "record.h"
#include "statement.h"
#include "utils.h"
#include "config.h"
#include "status.h"
#include "libpm.h"
#include "plist.h"
#include "serializer.h"

namespace storage {

class test_benchmark : public benchmark {
 public:
  test_benchmark(config _conf, unsigned int tid, database* _db, timer* _tm, struct static_info* _sp);

  void load();
  void execute();

  void sim_crash();

  void do_insert(engine* ee);
  void do_update(engine* ee);
  void do_delete(engine* ee);
  void do_read(engine* ee);

  // Table Ids
  static constexpr int TEST_TABLE_ID = 0;

  // Schema
  schema* test_table_schema;

  std::vector<int> zipf_dist;
  std::vector<double> uniform_dist;

  config conf;
  std::vector<int> update_field_ids;
  serializer sr;

  benchmark_type btype;

  unsigned int txn_id;
  unsigned int num_keys;
  unsigned int num_txns;
};

}
