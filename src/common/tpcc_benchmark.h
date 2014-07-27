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

  table* create_warehouse();
  table* create_district();
  table* create_item();
  table* create_customer();

  void load_warehouses(engine* ee);
  void load_district(engine* ee);
  void load_items(engine* ee);
  void load_customer(engine* ee);

  table* create_history();
  table* create_stock();
  table* create_orders();
  table* create_new_order();
  table* create_order_line();

  vector<double> uniform_dist;

  config& conf;
  benchmark_type btype;

  unsigned int txn_id;
  timer tm;

  // Table Ids
  const int ITEM_TABLE_ID = 0;
  const int WAREHOUSE_TABLE_ID = 1;
  const int DISTRICT_TABLE_ID = 2;
  const int CUSTOMER_TABLE_ID = 3;
  const int ORDERS_TABLE_ID = 4;
  const int ORDER_LINE_TABLE_ID = 5;
  const int NEW_ORDER_TABLE_ID = 6;
  const int HISTORY_TABLE_ID = 7;

  // Constants
  const int item_count = 3; // 100000
  const double item_min_price = 1.0;
  const double item_max_price = 100.0;
  const int item_name_len = 5;

  const int warehouse_count = 3; // 10
  const double warehouse_min_tax = 0.0;
  const double warehouse_max_tax = 0.2;
  const double state_len = 2;
  const double zip_len = 5;
  const double name_len = 5;

  const int districts_per_warehouse = 2; // 10

  const int customers_per_district = 10; // 3000
};

#endif /* TPCC_BENCHMARK_H_ */
