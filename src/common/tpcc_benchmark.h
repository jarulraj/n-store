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

  void do_delivery(engine* ee);
  void do_new_order(engine* ee);
  void do_order_status(engine* ee);
  void do_payment(engine* ee);
  void do_stock_level(engine* ee);

  // Table Ids
  const int ITEM_TABLE_ID = 0;
  const int WAREHOUSE_TABLE_ID = 1;
  const int DISTRICT_TABLE_ID = 2;
  const int CUSTOMER_TABLE_ID = 3;
  const int ORDERS_TABLE_ID = 4;
  const int ORDER_LINE_TABLE_ID = 5;
  const int NEW_ORDER_TABLE_ID = 6;
  const int HISTORY_TABLE_ID = 7;
  const int STOCK_TABLE_ID = 8;

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
  const double warehouse_initial_ytd = 300000.00f;

  const int districts_per_warehouse = 2; // 10
  const double district_initial_ytd = 30000.00f;

  const int customers_per_district = 10; // 3000
  const std::string customers_gcredit = "GC";
  const std::string customers_bcredit = "BC";
  const double customers_bad_credit_ratio = 0.1;
  const double customers_init_credit_lim = 50000.0;
  const double customers_min_discount = 0;
  const double customers_max_discount = 0.5;
  const double customers_init_balance = -10.0;
  const double customers_init_ytd = 10.0;
  const int customers_init_payment_cnt = 1;
  const int customers_init_delivery_cnt = 0;

  const double history_init_amount = 10.0;

  const int orders_min_ol_cnt = 5;
  const int orders_max_ol_cnt = 15;
  const int orders_init_all_local = 1;
  const int orders_null_carrier_id = 0;
  const int orders_min_carrier_id = 1;
  const int orders_max_carrier_id = 10;

  const int new_orders_per_district = 3; // 900

  const int order_line_init_quantity = 5;
  const int order_line_max_ol_quantity = 10;
  const double order_line_min_amount = 0.01;

  const double stock_original_ratio = 0.1;
  const int stock_min_quantity = 10;
  const int stock_max_quantity = 100;
  const int stock_dist_count = 10;

};

#endif /* TPCC_BENCHMARK_H_ */
