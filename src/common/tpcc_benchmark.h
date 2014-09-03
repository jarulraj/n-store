#pragma once

#include <cstdio>
#include <cstring>
#include <vector>
#include <set>

#include "config.h"
#include "statement.h"
#include "utils.h"
#include "status.h"
#include "benchmark.h"
#include "database.h"
#include "engine.h"
#include "timer.h"
#include "serializer.h"

namespace storage {

class tpcc_benchmark : public benchmark {
 public:
  tpcc_benchmark(config _conf, unsigned int tid, database* _db, timer* _tm,
                 struct static_info* _sp);

  void load();
  void execute();
  void sim_crash();

  table* create_warehouse();
  table* create_district();
  table* create_item();
  table* create_customer();

  void load_items(engine* ee);
  void load_warehouses(engine* ee);

  table* create_history();
  table* create_stock();
  table* create_orders();
  table* create_new_order();
  table* create_order_line();

  std::vector<double> uniform_dist;

  config conf;
  benchmark_type btype;

  serializer sr;

  unsigned int txn_id;
  unsigned int num_txns;

  void do_delivery(engine* ee);
  void do_new_order(engine* ee, bool finish = true);
  void do_order_status(engine* ee);
  void do_payment(engine* ee);
  void do_stock_level(engine* ee);

  // Table Ids
  static constexpr int ITEM_TABLE_ID = 0;
  static constexpr int WAREHOUSE_TABLE_ID = 1;
  static constexpr int DISTRICT_TABLE_ID = 2;
  static constexpr int CUSTOMER_TABLE_ID = 3;
  static constexpr int ORDERS_TABLE_ID = 4;
  static constexpr int ORDER_LINE_TABLE_ID = 5;
  static constexpr int NEW_ORDER_TABLE_ID = 6;
  static constexpr int HISTORY_TABLE_ID = 7;
  static constexpr int STOCK_TABLE_ID = 8;

  // Schema
  schema* item_table_schema;
  schema* warehouse_table_schema;
  schema* district_table_schema;
  schema* customer_table_schema;
  schema* history_table_schema;
  schema* orders_table_schema;
  schema* order_line_table_schema;
  schema* new_order_table_schema;
  schema* stock_table_schema;

  // Queries
  schema* customer_do_new_order_schema;
  schema* stock_table_do_stock_level_schema;

  // Constants
  int item_count = 1000;  // 100000
  static constexpr double item_min_price = 1.0;
  static constexpr double item_max_price = 100.0;
  static constexpr int item_name_len = 5;

  int warehouse_count = 1;  // 10
  static constexpr double warehouse_min_tax = 0.0;
  static constexpr double warehouse_max_tax = 0.2;
  static constexpr double state_len = 16;
  static constexpr double zip_len = 16;
  static constexpr double name_len = 32;
  static constexpr double warehouse_initial_ytd = 300000.00f;

  int districts_per_warehouse = 2;  // 10
  static constexpr double district_initial_ytd = 30000.00f;

  int customers_per_district = 3000;  // 3000
  const std::string customers_gcredit = "GC";
  const std::string customers_bcredit = "BC";
  static constexpr double customers_bad_credit_ratio = 0.1;
  static constexpr double customers_init_credit_lim = 50000.0;
  static constexpr double customers_min_discount = 0;
  static constexpr double customers_max_discount = 0.5;
  static constexpr double customers_init_balance = -10.0;
  static constexpr double customers_init_ytd = 10.0;
  static constexpr int customers_init_payment_cnt = 1;
  static constexpr int customers_init_delivery_cnt = 0;

  static constexpr double history_init_amount = 10.0;

  static constexpr int orders_min_ol_cnt = 5;
  static constexpr int orders_max_ol_cnt = 15;
  static constexpr int orders_init_all_local = 1;
  static constexpr int orders_null_carrier_id = 0;
  static constexpr int orders_min_carrier_id = 1;
  static constexpr int orders_max_carrier_id = 10;

  int new_orders_per_district = 900;  // 900

  static constexpr int order_line_init_quantity = 5;
  static constexpr int order_line_max_ol_quantity = 10;
  static constexpr double order_line_min_amount = 0.01;

  static constexpr double stock_original_ratio = 0.1;
  static constexpr int stock_min_quantity = 10;
  static constexpr int stock_max_quantity = 100;
  static constexpr int stock_dist_count = 10;

  static constexpr double payment_min_amount = 1.0;
  static constexpr double payment_max_amount = 5000.0;

  static constexpr int stock_min_threshold = 10;
  static constexpr int stock_max_threshold = 20;
};

}
