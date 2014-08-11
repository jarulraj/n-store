// TPCC BENCHMARK

#include "tpcc_benchmark.h"

#include <sys/types.h>
#include <ctime>
#include <iostream>
#include <string>

#include "field.h"
#include "libpm.h"
#include "plist.h"
#include "schema.h"
#include "table.h"
#include "table_index.h"

using namespace std;

tpcc_benchmark::tpcc_benchmark(config& _conf, unsigned int tid, database* _db,
                               timer* _tm, struct static_info* _sp)
    : benchmark(_conf, tid, _db, _tm, _sp),
      conf(_conf),
      txn_id(0) {

  btype = benchmark_type::TPCC;

  // Partition workload
  num_txns = conf.num_txns / conf.num_executors;

  // Initialization mode
  if (sp->init == 0) {
    cout << "Initialization Mode" << endl;

    sp->ptrs[0] = db;

    table* item_table = create_item();
    db->tables->push_back(item_table);
    table* warehouse_table = create_warehouse();
    db->tables->push_back(warehouse_table);
    table* district_table = create_district();
    db->tables->push_back(district_table);
    table* customer_table = create_customer();
    db->tables->push_back(customer_table);
    table* orders_table = create_orders();
    db->tables->push_back(orders_table);
    table* order_line_table = create_order_line();
    db->tables->push_back(order_line_table);
    table* new_order_table = create_new_order();
    db->tables->push_back(new_order_table);
    table* history_table = create_history();
    db->tables->push_back(history_table);
    table* stock_table = create_stock();
    db->tables->push_back(stock_table);

    sp->init = 1;
  } else {
    cout << "Recovery Mode " << endl;
    database* db = (database*) sp->ptrs[0];
    db->reset(conf, tid);
  }

  item_table_schema = db->tables->at(ITEM_TABLE_ID)->sptr;
  warehouse_table_schema = db->tables->at(WAREHOUSE_TABLE_ID)->sptr;
  district_table_schema = db->tables->at(DISTRICT_TABLE_ID)->sptr;
  customer_table_schema = db->tables->at(CUSTOMER_TABLE_ID)->sptr;
  orders_table_schema = db->tables->at(ORDERS_TABLE_ID)->sptr;
  order_line_table_schema = db->tables->at(ORDER_LINE_TABLE_ID)->sptr;
  new_order_table_schema = db->tables->at(NEW_ORDER_TABLE_ID)->sptr;
  history_table_schema = db->tables->at(HISTORY_TABLE_ID)->sptr;
  stock_table_schema = db->tables->at(STOCK_TABLE_ID)->sptr;

  uniform(uniform_dist, num_txns);

  if (conf.recovery) {
    item_count = 10000;
    warehouse_count = 2;
    districts_per_warehouse = 2;
    customers_per_district = 3000;
    new_orders_per_district = 900;
  }
}

// WAREHOUSE
class warehouse_record : public record {
 public:
  warehouse_record(schema* sptr, int w_id, const std::string& w_name,
                   const std::string& w_st, const std::string& w_zip,
                   const double w_tax, const double w_ytd)

      : record(sptr) {

    set_int(0, w_id);

    for (int itr = 1; itr <= 4; itr++) {
      set_varchar(itr, w_name);
    }

    set_varchar(5, w_st);
    set_varchar(6, w_zip);
    set_double(7, w_tax);
    set_double(8, w_ytd);
  }

};

table* tpcc_benchmark::create_warehouse() {

  vector<field_info> cols;
  off_t offset;

  /*
   CREATE TABLE WAREHOUSE (
   W_ID SMALLINT DEFAULT '0' NOT NULL,
   W_NAME VARCHAR(16) DEFAULT NULL,
   W_STREET_1 VARCHAR(32) DEFAULT NULL,
   W_STREET_2 VARCHAR(32) DEFAULT NULL,
   W_CITY VARCHAR(32) DEFAULT NULL,
   W_STATE VARCHAR(2) DEFAULT NULL,
   W_ZIP VARCHAR(9) DEFAULT NULL,
   W_TAX FLOAT DEFAULT NULL,
   W_YTD FLOAT DEFAULT NULL,
   CONSTRAINT W_PK_ARRAY PRIMARY KEY (W_ID)
   );
   */

  offset = 0;
  field_info field;

  field = field_info(offset, 10, 10, field_type::INTEGER, 1, 1);
  offset += field.ser_len;
  cols.push_back(field);

  for (int f_itr = 1; f_itr <= 4; f_itr++) {
    field = field_info(offset, 12, name_len, field_type::VARCHAR, 0, 1);
    offset += field.ser_len;
    cols.push_back(field);
  }

  field = field_info(offset, 12, zip_len, field_type::VARCHAR, 0, 1);
  offset += field.ser_len;
  cols.push_back(field);
  field = field_info(offset, 12, state_len, field_type::VARCHAR, 0, 1);
  offset += field.ser_len;
  cols.push_back(field);

  for (int f_itr = 7; f_itr <= 8; f_itr++) {
    field = field_info(offset, 15, 15, field_type::DOUBLE, 1, 1);
    offset += field.ser_len;
    cols.push_back(field);
  }

  // SCHEMA
  schema* warehouse_schema = new schema(cols);
  pmemalloc_activate(warehouse_schema);

  table* warehouse = new table("warehouse", warehouse_schema, 1, conf, sp);
  pmemalloc_activate(warehouse);

  // PRIMARY INDEX
  for (unsigned int itr = 1; itr < cols.size(); itr++) {
    cols[itr].enabled = 0;
  }

  schema* warehouse_index_schema = new schema(cols);
  pmemalloc_activate(warehouse_index_schema);

  table_index* key_index = new table_index(warehouse_index_schema, cols.size(),
                                           conf, sp);
  pmemalloc_activate(key_index);
  warehouse->indices->push_back(key_index);

  return warehouse;
}

// DISTRICT
class district_record : public record {
 public:
  district_record(schema* sptr, int d_id, int d_w_id, const std::string& d_name,
                  const std::string& d_st, const std::string& d_zip,
                  const double d_tax, const double d_ytd, int d_next_o_id)

      : record(sptr) {

    set_int(0, d_id);
    set_int(1, d_w_id);

    for (int itr = 2; itr <= 5; itr++) {
      set_varchar(itr, d_name);
    }

    set_varchar(6, d_st);
    set_varchar(7, d_zip);
    set_double(8, d_tax);
    set_double(9, d_ytd);
    set_int(10, d_next_o_id);
  }

};

table* tpcc_benchmark::create_district() {

  vector<field_info> cols;
  off_t offset;

  /*
   CREATE TABLE DISTRICT (
   D_ID TINYINT DEFAULT '0' NOT NULL,
   D_W_ID SMALLINT DEFAULT '0' NOT NULL REFERENCES WAREHOUSE (W_ID),
   D_NAME VARCHAR(16) DEFAULT NULL,
   D_STREET_1 VARCHAR(32) DEFAULT NULL,
   D_STREET_2 VARCHAR(32) DEFAULT NULL,
   D_CITY VARCHAR(32) DEFAULT NULL,
   D_STATE VARCHAR(2) DEFAULT NULL,
   D_ZIP VARCHAR(9) DEFAULT NULL,
   D_TAX FLOAT DEFAULT NULL,
   D_YTD FLOAT DEFAULT NULL,
   D_NEXT_O_ID INT DEFAULT NULL,
   PRIMARY KEY (D_W_ID,D_ID)
   );
   */

  offset = 0;
  field_info field;

  field = field_info(offset, 10, 10, field_type::INTEGER, 1, 1);
  offset += field.ser_len;
  cols.push_back(field);
  field = field_info(offset, 10, 10, field_type::INTEGER, 1, 1);
  offset += field.ser_len;
  cols.push_back(field);

  for (int f_itr = 2; f_itr <= 5; f_itr++) {
    field = field_info(offset, 12, name_len, field_type::VARCHAR, 0, 1);
    offset += field.ser_len;
    cols.push_back(field);
  }
  for (int f_itr = 6; f_itr <= 7; f_itr++) {
    field = field_info(offset, 12, name_len, field_type::VARCHAR, 0, 1);
    offset += field.ser_len;
    cols.push_back(field);
  }
  for (int f_itr = 8; f_itr <= 9; f_itr++) {
    field = field_info(offset, 15, 15, field_type::DOUBLE, 1, 1);
    offset += field.ser_len;
    cols.push_back(field);
  }

  field = field_info(offset, 10, 10, field_type::INTEGER, 1, 1);
  offset += field.ser_len;
  cols.push_back(field);

  // SCHEMA
  schema* district_schema = new schema(cols);
  pmemalloc_activate(district_schema);

  table* district = new table("district", district_schema, 1, conf, sp);
  pmemalloc_activate(district);

  // PRIMARY INDEX
  for (unsigned int itr = 2; itr < cols.size(); itr++) {
    cols[itr].enabled = 0;
  }

  schema* district_index_schema = new schema(cols);
  pmemalloc_activate(district_index_schema);

  table_index* key_index = new table_index(district_index_schema, cols.size(),
                                           conf, sp);
  pmemalloc_activate(key_index);
  district->indices->push_back(key_index);

  return district;
}

// ITEM
class item_record : public record {
 public:
  item_record(schema* sptr, int i_id, int i_im_id, const std::string& i_name,
              const double i_price)
      : record(sptr) {

    set_int(0, i_id);
    set_int(1, i_im_id);
    set_varchar(2, i_name);
    set_double(3, i_price);
    set_varchar(4, i_name);

  }

};

table* tpcc_benchmark::create_item() {

  vector<field_info> cols;
  off_t offset;

  /*
   CREATE TABLE ITEM (
   I_ID INTEGER DEFAULT '0' NOT NULL,
   I_IM_ID INTEGER DEFAULT NULL,
   I_NAME VARCHAR(32) DEFAULT NULL,
   I_PRICE FLOAT DEFAULT NULL,
   I_DATA VARCHAR(64) DEFAULT NULL,
   CONSTRAINT I_PK_ARRAY PRIMARY KEY (I_ID)
   );
   */

  offset = 0;
  field_info field;

  field = field_info(offset, 10, 10, field_type::INTEGER, 1, 1);
  offset += field.ser_len;
  cols.push_back(field);
  field = field_info(offset, 10, 10, field_type::INTEGER, 1, 1);
  offset += field.ser_len;
  cols.push_back(field);
  field = field_info(offset, 12, name_len, field_type::VARCHAR, 0, 1);
  offset += field.ser_len;
  cols.push_back(field);
  field = field_info(offset, 15, 15, field_type::DOUBLE, 1, 1);
  offset += field.ser_len;
  cols.push_back(field);
  field = field_info(offset, 12, 64, field_type::VARCHAR, 0, 1);
  offset += field.ser_len;
  cols.push_back(field);

  // SCHEMA
  schema* item_schema = new schema(cols);
  pmemalloc_activate(item_schema);

  table* item = new table("item", item_schema, 1, conf, sp);
  pmemalloc_activate(item);

  // PRIMARY INDEX
  for (unsigned int itr = 1; itr < cols.size(); itr++) {
    cols[itr].enabled = 0;
  }

  schema* item_index_schema = new schema(cols);
  pmemalloc_activate(item_index_schema);

  table_index* key_index = new table_index(item_index_schema, cols.size(), conf,
                                           sp);
  pmemalloc_activate(key_index);
  item->indices->push_back(key_index);

  return item;
}

// CUSTOMER
class customer_record : public record {
 public:
  customer_record(schema* sptr, int c_id, int c_d_id, int c_w_id,
                  const std::string& c_name, const std::string& c_st,
                  const std::string& c_zip, const std::string& c_credit,
                  const double c_credit_lim, const double c_ts,
                  const double c_discount, const double c_balance,
                  const double c_ytd, const int c_payment_cnt,
                  const int c_delivery_cnt, std::string c_data)
      : record(sptr) {

    set_int(0, c_id);
    set_int(1, c_d_id);
    set_int(2, c_w_id);

    for (int itr = 3; itr <= 8; itr++) {
      set_varchar(itr, c_name);
    }

    set_varchar(9, c_st);
    set_varchar(10, c_zip);
    set_varchar(11, c_name);

    set_double(12, c_ts);
    set_varchar(13, c_credit);

    set_double(14, c_credit_lim);
    set_double(15, c_discount);
    set_double(16, c_balance);
    set_double(17, c_ytd);

    set_int(18, c_payment_cnt);
    set_int(19, c_delivery_cnt);

    set_varchar(20, c_data);
  }

};

table* tpcc_benchmark::create_customer() {

  vector<field_info> cols;
  off_t offset;

  /*
   CREATE TABLE CUSTOMER (
   C_ID INTEGER DEFAULT '0' NOT NULL,
   C_D_ID TINYINT DEFAULT '0' NOT NULL,
   C_W_ID SMALLINT DEFAULT '0' NOT NULL,
   C_FIRST VARCHAR(32) DEFAULT NULL,
   C_MIDDLE VARCHAR(2) DEFAULT NULL,
   C_LAST VARCHAR(32) DEFAULT NULL,
   C_STREET_1 VARCHAR(32) DEFAULT NULL,
   C_STREET_2 VARCHAR(32) DEFAULT NULL,
   C_CITY VARCHAR(32) DEFAULT NULL,
   C_STATE VARCHAR(2) DEFAULT NULL,
   C_ZIP VARCHAR(9) DEFAULT NULL,
   C_PHONE VARCHAR(32) DEFAULT NULL,
   C_SINCE TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
   C_CREDIT VARCHAR(2) DEFAULT NULL,
   C_CREDIT_LIM FLOAT DEFAULT NULL,
   C_DISCOUNT FLOAT DEFAULT NULL,
   C_BALANCE FLOAT DEFAULT NULL,
   C_YTD_PAYMENT FLOAT DEFAULT NULL,
   C_PAYMENT_CNT INTEGER DEFAULT NULL,
   C_DELIVERY_CNT INTEGER DEFAULT NULL,
   C_DATA VARCHAR(500),
   PRIMARY KEY (C_W_ID,C_D_ID,C_ID),
   UNIQUE (C_W_ID,C_D_ID,C_LAST,C_FIRST),
   CONSTRAINT C_FKEY_D FOREIGN KEY (C_D_ID, C_W_ID) REFERENCES DISTRICT (D_ID, D_W_ID)
   );
   CREATE INDEX IDX_CUSTOMER ON CUSTOMER (C_W_ID,C_D_ID,C_LAST);
   */

  offset = 0;
  field_info field;

  for (int f_itr = 0; f_itr <= 2; f_itr++) {
    field = field_info(offset, 10, 10, field_type::INTEGER, 1, 1);
    offset += field.ser_len;
    cols.push_back(field);
  }

  for (int f_itr = 3; f_itr <= 8; f_itr++) {
    field = field_info(offset, 12, name_len, field_type::VARCHAR, 0, 1);
    offset += field.ser_len;
    cols.push_back(field);
  }

  field = field_info(offset, 12, zip_len, field_type::VARCHAR, 0, 1);
  offset += field.ser_len;
  cols.push_back(field);
  field = field_info(offset, 12, state_len, field_type::VARCHAR, 0, 1);
  offset += field.ser_len;
  cols.push_back(field);
  field = field_info(offset, 12, 32, field_type::VARCHAR, 0, 1);
  offset += field.ser_len;
  cols.push_back(field);
  field = field_info(offset, 15, 15, field_type::DOUBLE, 1, 1);
  offset += field.ser_len;
  cols.push_back(field);
  field = field_info(offset, 12, 2, field_type::VARCHAR, 0, 1);
  offset += field.ser_len;
  cols.push_back(field);

  for (int f_itr = 14; f_itr <= 17; f_itr++) {
    field = field_info(offset, 15, 15, field_type::DOUBLE, 1, 1);
    offset += field.ser_len;
    cols.push_back(field);
  }
  for (int f_itr = 18; f_itr <= 19; f_itr++) {
    field = field_info(offset, 10, 10, field_type::INTEGER, 1, 1);
    offset += field.ser_len;
    cols.push_back(field);
  }
  field = field_info(offset, 12, 500, field_type::VARCHAR, 0, 1);
  offset += field.ser_len;
  cols.push_back(field);

  // SCHEMA
  schema* customer_schema = new schema(cols);
  pmemalloc_activate(customer_schema);

  table* customer = new table("customer", customer_schema, 2, conf, sp);
  pmemalloc_activate(customer);

  // PRIMARY INDEX
  for (unsigned int itr = 3; itr < cols.size(); itr++) {
    cols[itr].enabled = 0;
  }

  schema* customer_index_schema = new schema(cols);
  pmemalloc_activate(customer_index_schema);

  table_index* p_index = new table_index(customer_index_schema, cols.size(),
                                         conf, sp);
  pmemalloc_activate(p_index);
  customer->indices->push_back(p_index);

  // SECONDARY INDEX
  cols[0].enabled = 0;
  cols[3].enabled = 1;

  schema* customer_name_index_schema = new schema(cols);
  pmemalloc_activate(customer_name_index_schema);

  table_index* s_index = new table_index(customer_name_index_schema,
                                         cols.size(), conf, sp);
  pmemalloc_activate(s_index);
  customer->indices->push_back(s_index);

  // QUERY SCHEMAS
  for (unsigned int itr = 0; itr < cols.size(); itr++)
    cols[itr].enabled = 0;
  cols[3].enabled = 1;
  cols[13].enabled = 1;
  cols[15].enabled = 1;

  customer_do_new_order_schema = new schema(cols);

  return customer;
}

// HISTORY

class history_record : public record {
 public:
  history_record(schema* sptr, int h_c_id, int h_c_d_id, int h_c_w_id,
                 int h_d_id, int h_w_id, const double h_ts,
                 const double h_amount, const std::string& h_data)
      : record(sptr) {

    set_int(0, h_c_id);
    set_int(1, h_c_d_id);
    set_int(2, h_c_w_id);
    set_int(3, h_d_id);
    set_int(4, h_w_id);

    set_double(5, h_ts);
    set_double(6, h_amount);
    set_varchar(7, h_data);
  }

};

table* tpcc_benchmark::create_history() {

  vector<field_info> cols;
  off_t offset;

  /*
   CREATE TABLE HISTORY (
   H_C_ID INTEGER DEFAULT NULL,
   H_C_D_ID TINYINT DEFAULT NULL,
   H_C_W_ID SMALLINT DEFAULT NULL,
   H_D_ID TINYINT DEFAULT NULL,
   H_W_ID SMALLINT DEFAULT '0' NOT NULL,
   H_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
   H_AMOUNT FLOAT DEFAULT NULL,
   H_DATA VARCHAR(32) DEFAULT NULL,
   CONSTRAINT H_FKEY_C FOREIGN KEY (H_C_ID, H_C_D_ID, H_C_W_ID) REFERENCES CUSTOMER (C_ID, C_D_ID, C_W_ID),
   CONSTRAINT H_FKEY_D FOREIGN KEY (H_D_ID, H_W_ID) REFERENCES DISTRICT (D_ID, D_W_ID)
   );
   */

  offset = 0;
  field_info field;

  for (int f_itr = 0; f_itr <= 4; f_itr++) {
    field = field_info(offset, 10, 10, field_type::INTEGER, 1, 1);
    offset += field.ser_len;
    cols.push_back(field);
  }
  for (int f_itr = 5; f_itr <= 6; f_itr++) {
    field = field_info(offset, 15, 15, field_type::DOUBLE, 1, 1);
    offset += field.ser_len;
    cols.push_back(field);
  }
  field = field_info(offset, 12, 32, field_type::VARCHAR, 0, 1);
  offset += field.ser_len;
  cols.push_back(field);

  // SCHEMA
  schema* history_schema = new schema(cols);
  pmemalloc_activate(history_schema);

  table* history = new table("history", history_schema, 1, conf, sp);
  pmemalloc_activate(history);

  // PRIMARY INDEX
  for (unsigned int itr = 5; itr < cols.size(); itr++) {
    cols[itr].enabled = 0;
  }

  schema* history_index_schema = new schema(cols);
  pmemalloc_activate(history_index_schema);

  table_index* p_index = new table_index(history_index_schema, cols.size(),
                                         conf, sp);
  pmemalloc_activate(p_index);
  history->indices->push_back(p_index);

  return history;
}

// STOCK

class stock_record : public record {
 public:
  stock_record(schema* sptr, int s_i_id, int s_w_id, int s_quantity,
               vector<std::string> s_dist, int s_ytd, int s_order_cnt,
               int s_remote_cnt, const std::string& s_data)
      : record(sptr) {

    set_int(0, s_i_id);
    set_int(1, s_w_id);
    set_int(2, s_quantity);

    for (int f_itr = 3; f_itr <= 12; f_itr++) {
      int s_dist_itr = f_itr - 3;
      set_varchar(f_itr, s_dist[s_dist_itr]);
    }

    set_int(13, s_ytd);
    set_int(14, s_order_cnt);
    set_int(15, s_remote_cnt);
    set_varchar(16, s_data);
  }

};

table* tpcc_benchmark::create_stock() {

  vector<field_info> cols;
  off_t offset;

  /*
   CREATE TABLE STOCK (
   S_I_ID INTEGER DEFAULT '0' NOT NULL REFERENCES ITEM (I_ID),
   S_W_ID SMALLINT DEFAULT '0 ' NOT NULL REFERENCES WAREHOUSE (W_ID),
   S_QUANTITY INTEGER DEFAULT '0' NOT NULL,
   S_DIST_01 VARCHAR(32) DEFAULT NULL,
   S_DIST_02 VARCHAR(32) DEFAULT NULL,
   S_DIST_03 VARCHAR(32) DEFAULT NULL,
   S_DIST_04 VARCHAR(32) DEFAULT NULL,
   S_DIST_05 VARCHAR(32) DEFAULT NULL,
   S_DIST_06 VARCHAR(32) DEFAULT NULL,
   S_DIST_07 VARCHAR(32) DEFAULT NULL,
   S_DIST_08 VARCHAR(32) DEFAULT NULL,
   S_DIST_09 VARCHAR(32) DEFAULT NULL,
   S_DIST_10 VARCHAR(32) DEFAULT NULL,
   S_YTD INTEGER DEFAULT NULL,
   S_ORDER_CNT INTEGER DEFAULT NULL,
   S_REMOTE_CNT INTEGER DEFAULT NULL,
   S_DATA VARCHAR(64) DEFAULT NULL,
   PRIMARY KEY (S_W_ID,S_I_ID)
   );
   */

  offset = 0;
  field_info field;

  for (int f_itr = 0; f_itr <= 2; f_itr++) {
    field = field_info(offset, 10, 10, field_type::INTEGER, 1, 1);
    offset += field.ser_len;
    cols.push_back(field);
  }
  for (int f_itr = 3; f_itr <= 12; f_itr++) {
    field = field_info(offset, 12, 32, field_type::VARCHAR, 0, 1);
    offset += field.ser_len;
    cols.push_back(field);
  }
  for (int f_itr = 13; f_itr <= 15; f_itr++) {
    field = field_info(offset, 10, 10, field_type::INTEGER, 1, 1);
    offset += field.ser_len;
    cols.push_back(field);
  }
  field = field_info(offset, 12, 64, field_type::VARCHAR, 0, 1);
  offset += field.ser_len;
  cols.push_back(field);

  // SCHEMA
  schema* stock_schema = new schema(cols);
  pmemalloc_activate(stock_schema);

  table* stock = new table("stock", stock_schema, 1, conf, sp);
  pmemalloc_activate(stock);

  // PRIMARY INDEX
  for (unsigned int itr = 2; itr < cols.size(); itr++) {
    cols[itr].enabled = 0;
  }

  schema* stock_index_schema = new schema(cols);
  pmemalloc_activate(stock_index_schema);

  table_index* p_index = new table_index(stock_index_schema, cols.size(), conf,
                                         sp);
  pmemalloc_activate(p_index);
  stock->indices->push_back(p_index);

  // QUERY SCHEMA
  for (unsigned int itr = 0; itr < cols.size(); itr++)
    cols[itr].enabled = 0;
  cols[2].enabled = 1;

  stock_table_do_stock_level_schema = new schema(cols);

  return stock;
}

// ORDERS

class orders_record : public record {
 public:
  orders_record(schema* sptr, int o_id, int o_c_id, int o_d_id, int o_w_id,
                double o_entry_ts, int o_carrier_id, int o_ol_cnt,
                int o_all_local)
      : record(sptr) {

    set_int(0, o_id);
    set_int(1, o_c_id);
    set_int(2, o_d_id);
    set_int(3, o_w_id);

    set_double(4, o_entry_ts);

    set_int(5, o_carrier_id);
    set_int(6, o_ol_cnt);
    set_int(7, o_all_local);
  }

};

table* tpcc_benchmark::create_orders() {

  vector<field_info> cols;
  off_t offset;

  /*
   CREATE TABLE ORDERS (
   O_ID INTEGER DEFAULT '0' NOT NULL,
   O_C_ID INTEGER DEFAULT NULL,
   O_D_ID TINYINT DEFAULT '0' NOT NULL,
   O_W_ID SMALLINT DEFAULT '0' NOT NULL,
   O_ENTRY_D TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
   O_CARRIER_ID INTEGER DEFAULT NULL,
   O_OL_CNT INTEGER DEFAULT NULL,
   O_ALL_LOCAL INTEGER DEFAULT NULL,
   PRIMARY KEY (O_W_ID,O_D_ID,O_ID),
   UNIQUE (O_W_ID,O_D_ID,O_C_ID,O_ID),
   CONSTRAINT O_FKEY_C FOREIGN KEY (O_C_ID, O_D_ID, O_W_ID) REFERENCES CUSTOMER (C_ID, C_D_ID, C_W_ID)
   );
   CREATE INDEX IDX_ORDERS ON ORDERS (O_W_ID,O_D_ID,O_C_ID);
   */

  offset = 0;
  field_info field;

  for (int f_itr = 0; f_itr <= 3; f_itr++) {
    field = field_info(offset, 10, 10, field_type::INTEGER, 1, 1);
    offset += field.ser_len;
    cols.push_back(field);
  }

  field = field_info(offset, 15, 15, field_type::DOUBLE, 1, 1);
  offset += field.ser_len;
  cols.push_back(field);

  for (int f_itr = 5; f_itr <= 7; f_itr++) {
    field = field_info(offset, 10, 10, field_type::INTEGER, 1, 1);
    offset += field.ser_len;
    cols.push_back(field);
  }

  // SCHEMA
  schema* orders_schema = new schema(cols);
  pmemalloc_activate(orders_schema);

  table* orders = new table("orders", orders_schema, 2, conf, sp);
  pmemalloc_activate(orders);

  // PRIMARY INDEX
  for (unsigned int itr = 4; itr < cols.size(); itr++) {
    cols[itr].enabled = 0;
  }
  cols[1].enabled = 0;

  schema* p_index_schema = new schema(cols);
  pmemalloc_activate(p_index_schema);

  table_index* p_index = new table_index(p_index_schema, cols.size(), conf, sp);
  pmemalloc_activate(p_index);
  orders->indices->push_back(p_index);

  // SECONDARY INDEX
  cols[0].enabled = 0;
  cols[1].enabled = 1;

  schema* s_index_schema = new schema(cols);
  pmemalloc_activate(s_index_schema);

  table_index* s_index = new table_index(s_index_schema, cols.size(), conf, sp);
  pmemalloc_activate(s_index);
  orders->indices->push_back(s_index);

  return orders;
}

// NEW_ORDER

class new_order_record : public record {
 public:
  new_order_record(schema* sptr, int no_o_id, int no_d_id, int no_w_id)
      : record(sptr) {

    set_int(0, no_o_id);
    set_int(1, no_d_id);
    set_int(2, no_w_id);

  }

};

table* tpcc_benchmark::create_new_order() {

  vector<field_info> cols;
  off_t offset;

  /*
   CREATE TABLE NEW_ORDER (
   NO_O_ID INTEGER DEFAULT '0' NOT NULL,
   NO_D_ID TINYINT DEFAULT '0' NOT NULL,
   NO_W_ID SMALLINT DEFAULT '0' NOT NULL,
   CONSTRAINT NO_PK_TREE PRIMARY KEY (NO_D_ID,NO_W_ID,NO_O_ID),
   CONSTRAINT NO_FKEY_O FOREIGN KEY (NO_O_ID, NO_D_ID, NO_W_ID) REFERENCES ORDERS (O_ID, O_D_ID, O_W_ID)
   );
   */

  offset = 0;
  field_info field;

  for (int f_itr = 0; f_itr <= 2; f_itr++) {
    field = field_info(offset, 10, 10, field_type::INTEGER, 1, 1);
    offset += field.ser_len;
    cols.push_back(field);
  }

  // SCHEMA
  schema* new_order_schema = new schema(cols);
  pmemalloc_activate(new_order_schema);

  table* new_order = new table("new_order", new_order_schema, 1, conf, sp);
  pmemalloc_activate(new_order);

  // PRIMARY INDEX
  // XXX alter pkey
  cols[0].enabled = 0;

  schema* new_order_index_schema = new schema(cols);
  pmemalloc_activate(new_order_index_schema);

  table_index* new_order_index = new table_index(new_order_index_schema,
                                                 cols.size(), conf, sp);
  pmemalloc_activate(new_order_index);
  new_order->indices->push_back(new_order_index);

  return new_order;
}

// ORDER_LINE

class order_line_record : public record {
 public:
  order_line_record(schema* sptr, int ol_o_id, int ol_d_id, int ol_w_id,
                    int ol_number, int ol_i_id, int ol_supply_w_id,
                    double ol_delivery_ts, int ol_quantity, double ol_amount,
                    const std::string& ol_info)
      : record(sptr) {

    set_int(0, ol_o_id);
    set_int(1, ol_d_id);
    set_int(2, ol_w_id);
    set_int(3, ol_number);

    set_int(4, ol_i_id);
    set_int(5, ol_supply_w_id);

    set_double(6, ol_delivery_ts);
    set_int(7, ol_quantity);

    set_double(8, ol_amount);
    set_varchar(9, ol_info);
  }

};

table* tpcc_benchmark::create_order_line() {

  vector<field_info> cols;
  off_t offset;

  /*
   CREATE TABLE ORDER_LINE (
   OL_O_ID INTEGER DEFAULT '0' NOT NULL,
   OL_D_ID TINYINT DEFAULT '0' NOT NULL,
   OL_W_ID SMALLINT DEFAULT '0' NOT NULL,
   OL_NUMBER INTEGER DEFAULT '0' NOT NULL,
   OL_I_ID INTEGER DEFAULT NULL,
   OL_SUPPLY_W_ID SMALLINT DEFAULT NULL,
   OL_DELIVERY_D TIMESTAMP DEFAULT NULL,
   OL_QUANTITY INTEGER DEFAULT NULL,
   OL_AMOUNT FLOAT DEFAULT NULL,
   OL_DIST_INFO VARCHAR(32) DEFAULT NULL,
   PRIMARY KEY (OL_W_ID,OL_D_ID,OL_O_ID,OL_NUMBER),
   CONSTRAINT OL_FKEY_O FOREIGN KEY (OL_O_ID, OL_D_ID, OL_W_ID) REFERENCES ORDERS (O_ID, O_D_ID, O_W_ID),
   CONSTRAINT OL_FKEY_S FOREIGN KEY (OL_I_ID, OL_SUPPLY_W_ID) REFERENCES STOCK (S_I_ID, S_W_ID)
   );
   CREATE INDEX IDX_ORDER_LINE_TREE ON ORDER_LINE (OL_W_ID,OL_D_ID,OL_O_ID);
   */

  offset = 0;
  field_info field;

  for (int f_itr = 0; f_itr <= 5; f_itr++) {
    field = field_info(offset, 10, 10, field_type::INTEGER, 1, 1);
    offset += field.ser_len;
    cols.push_back(field);
  }

  field = field_info(offset, 15, 15, field_type::DOUBLE, 1, 1);
  offset += field.ser_len;
  cols.push_back(field);
  field = field_info(offset, 10, 10, field_type::INTEGER, 1, 1);
  offset += field.ser_len;
  cols.push_back(field);
  field = field_info(offset, 15, 15, field_type::DOUBLE, 1, 1);
  offset += field.ser_len;
  cols.push_back(field);
  field = field_info(offset, 12, 32, field_type::VARCHAR, 0, 1);
  offset += field.ser_len;
  cols.push_back(field);

  // SCHEMA
  schema* order_line_schema = new schema(cols);
  pmemalloc_activate(order_line_schema);

  table* order_line = new table("order_line", order_line_schema, 2, conf, sp);
  pmemalloc_activate(order_line);

  // PRIMARY INDEX
  for (unsigned int itr = 4; itr < cols.size(); itr++) {
    cols[itr].enabled = 0;
  }

  schema* p_index_schema = new schema(cols);
  pmemalloc_activate(p_index_schema);

  table_index* p_index = new table_index(p_index_schema, cols.size(), conf, sp);
  pmemalloc_activate(p_index);
  order_line->indices->push_back(p_index);

  // SECONDARY INDEX
  cols[3].enabled = 0;

  schema* s_index_schema = new schema(cols);
  pmemalloc_activate(s_index_schema);

  table_index* s_index = new table_index(s_index_schema, cols.size(), conf, sp);
  pmemalloc_activate(s_index);
  order_line->indices->push_back(s_index);

  return order_line;
}

void tpcc_benchmark::load_items(engine* ee) {
  int num_items = item_count;  //100000

  schema* item_table_schema = db->tables->at(ITEM_TABLE_ID)->sptr;
  int i_itr;

  ee->txn_begin();

  for (i_itr = 0; i_itr < num_items; i_itr++) {

    if (i_itr % conf.load_batch_size == 0) {
      ee->txn_end(true);
      txn_id++;
      ee->txn_begin();
    }

    int i_im_id = i_itr * 10;
    std::string name = get_rand_astring(name_len);
    double price = get_rand_double(item_min_price, item_max_price);

    record* rec_ptr = new item_record(item_table_schema, i_itr, i_im_id, name,
                                      price);

    std::string key_str = serialize(rec_ptr, item_table_schema);
    //LOG_INFO("item :: %s ", key_str.c_str());

    statement st(txn_id, operation_type::Insert, ITEM_TABLE_ID, rec_ptr);

    ee->load(st);
  }

  ee->txn_end(true);

}

void tpcc_benchmark::load_warehouses(engine* ee) {
  int num_warehouses = warehouse_count;

  schema* warehouse_table_schema = db->tables->at(WAREHOUSE_TABLE_ID)->sptr;
  schema* district_table_schema = db->tables->at(DISTRICT_TABLE_ID)->sptr;
  schema* customer_table_schema = db->tables->at(CUSTOMER_TABLE_ID)->sptr;
  schema* history_table_schema = db->tables->at(HISTORY_TABLE_ID)->sptr;
  schema* orders_table_schema = db->tables->at(ORDERS_TABLE_ID)->sptr;
  schema* order_line_table_schema = db->tables->at(ORDER_LINE_TABLE_ID)->sptr;
  schema* new_order_table_schema = db->tables->at(NEW_ORDER_TABLE_ID)->sptr;
  schema* stock_table_schema = db->tables->at(STOCK_TABLE_ID)->sptr;

  int w_itr, d_itr, c_itr, o_itr, ol_itr, s_i_itr;
  statement st;
  std::string log_str;
  status ss(num_warehouses * districts_per_warehouse);

  // WAREHOUSE
  for (w_itr = 0; w_itr < num_warehouses; w_itr++) {
    txn_id++;
    ee->txn_begin();

    std::string name = get_rand_astring(name_len);
    std::string state = get_rand_astring(state_len);
    std::string zip = get_rand_astring(zip_len);
    double w_tax = get_rand_double(warehouse_min_tax, warehouse_max_tax);

    record* warehouse_rec_ptr = new warehouse_record(warehouse_table_schema,
                                                     w_itr, name, zip, state,
                                                     w_tax,
                                                     warehouse_initial_ytd);

    log_str = serialize(warehouse_rec_ptr, warehouse_table_schema);
    //LOG_INFO("warehouse :: %s ", log_str.c_str());

    st = statement(txn_id, operation_type::Insert, WAREHOUSE_TABLE_ID,
                   warehouse_rec_ptr);

    ee->load(st);
    ee->txn_end(true);

    // DISTRICTS
    for (d_itr = 0; d_itr < districts_per_warehouse; d_itr++) {

      txn_id++;
      ee->txn_begin();

      std::string name = get_rand_astring(name_len);
      std::string state = get_rand_astring(state_len);
      std::string zip = get_rand_astring(zip_len);
      double d_tax = get_rand_double(warehouse_min_tax, warehouse_max_tax);
      int next_d_o_id = customers_per_district + 1;

      record* district_rec_ptr = new district_record(district_table_schema,
                                                     d_itr, w_itr, name, zip,
                                                     state, d_tax,
                                                     district_initial_ytd,
                                                     next_d_o_id);

      log_str = serialize(district_rec_ptr, district_table_schema);
      //LOG_INFO("district :: %s", log_str.c_str());

      st = statement(txn_id, operation_type::Insert, DISTRICT_TABLE_ID,
                     district_rec_ptr);

      ee->load(st);

      ee->txn_end(true);

      txn_id++;
      ee->txn_begin();

      // CUSTOMERS
      for (c_itr = 0; c_itr < customers_per_district; c_itr++) {

        bool bad_credit = get_rand_bool(customers_bad_credit_ratio);
        std::string c_name = get_rand_astring(name_len);
        std::string c_state = get_rand_astring(state_len);
        std::string c_zip = get_rand_astring(zip_len);
        std::string c_credit = (
            bad_credit ? customers_bcredit : customers_gcredit);
        double c_ts = static_cast<double>(time(NULL));
        double c_discount = get_rand_double(customers_min_discount,
                                            customers_max_discount);

        record* customer_rec_ptr = new customer_record(
            customer_table_schema, c_itr, d_itr, w_itr, c_name, c_state, c_zip,
            c_credit, customers_init_credit_lim, c_ts, c_discount,
            customers_init_balance, customers_init_ytd,
            customers_init_payment_cnt, customers_init_delivery_cnt, c_name);

        log_str = serialize(customer_rec_ptr, customer_table_schema);
        //LOG_INFO("customer :: %s", log_str.c_str());

        st = statement(txn_id, operation_type::Insert, CUSTOMER_TABLE_ID,
                       customer_rec_ptr);

        ee->load(st);

        // HISTORY

        txn_id++;

        int h_w_id = w_itr;
        int h_d_id = d_itr;
        std::string h_data = get_rand_astring(name_len);

        record* history_rec_ptr = new history_record(history_table_schema,
                                                     c_itr, d_itr, w_itr,
                                                     h_w_id, h_d_id, c_ts,
                                                     history_init_amount,
                                                     h_data);

        log_str = serialize(history_rec_ptr, history_table_schema);
        //LOG_INFO("history :: %s ", log_str.c_str());

        st = statement(txn_id, operation_type::Insert, HISTORY_TABLE_ID,
                       history_rec_ptr);

        ee->load(st);
      }

      ee->txn_end(true);

      ee->txn_begin();

      // ORDERS
      for (o_itr = 0; o_itr < customers_per_district; o_itr++) {
        txn_id++;

        if (o_itr % conf.load_batch_size == 0) {
          ee->txn_end(true);
          txn_id++;
          ee->txn_begin();
        }

        bool new_order = (o_itr
            > (customers_per_district - new_orders_per_district));
        int o_ol_cnt = get_rand_int(orders_min_ol_cnt, orders_max_ol_cnt);
        int c_id = get_rand_int(0, customers_per_district);
        double o_ts = static_cast<double>(time(NULL));
        int o_carrier_id = orders_null_carrier_id;

        if (!new_order)
          o_carrier_id = get_rand_int(orders_min_carrier_id,
                                      orders_max_carrier_id);

        record* orders_rec_ptr = new orders_record(orders_table_schema, o_itr,
                                                   c_id, d_itr, w_itr, o_ts,
                                                   o_carrier_id, o_ol_cnt,
                                                   orders_init_all_local);

        log_str = serialize(orders_rec_ptr, orders_table_schema);
        //LOG_INFO("orders ::%s", log_str.c_str());

        st = statement(txn_id, operation_type::Insert, ORDERS_TABLE_ID,
                       orders_rec_ptr);

        ee->load(st);

        // NEW_ORDER

        if (new_order) {
          txn_id++;

          record* new_order_rec_ptr = new new_order_record(
              new_order_table_schema, o_itr, d_itr, w_itr);

          log_str = serialize(new_order_rec_ptr, new_order_table_schema);
          LOG_INFO("new_order ::%s", log_str.c_str());

          st = statement(txn_id, operation_type::Insert, NEW_ORDER_TABLE_ID,
                         new_order_rec_ptr);

          ee->load(st);
        }

        // ORDER_LINE
        for (ol_itr = 0; ol_itr < o_ol_cnt; ol_itr++) {
          txn_id++;

          int ol_i_id = get_rand_int(0, item_count);
          int ol_supply_w_id = w_itr;
          double ol_delivery_ts = static_cast<double>(time(NULL));
          int ol_quantity = order_line_init_quantity;
          std::string ol_data = get_rand_astring(name_len);
          double ol_amount = 0.0;

          if (new_order) {
            ol_amount = get_rand_double(
                order_line_min_amount,
                order_line_max_ol_quantity * item_max_price);
            ol_delivery_ts = 0;
          }

          record* order_line_rec_ptr = new order_line_record(
              order_line_table_schema, o_itr, d_itr, w_itr, ol_itr, ol_i_id,
              ol_supply_w_id, ol_delivery_ts, ol_quantity, ol_amount, ol_data);

          log_str = serialize(order_line_rec_ptr, order_line_table_schema);
          //LOG_INFO("order_line ::%s", log_str.c_str());

          st = statement(txn_id, operation_type::Insert, ORDER_LINE_TABLE_ID,
                         order_line_rec_ptr);

          ee->load(st);
        }

      }

      ee->txn_end(true);

      ss.display();
    }

    txn_id++;
    ee->txn_begin();

    // STOCK
    for (s_i_itr = 0; s_i_itr < item_count; s_i_itr++) {

      if (s_i_itr % conf.load_batch_size == 0) {
        ee->txn_end(true);
        txn_id++;
        ee->txn_begin();
      }

      int s_quantity = get_rand_int(stock_min_quantity, stock_max_quantity);
      int s_ytd = 0;
      int s_order_cnt = 0;
      int s_remote_cnt = 0;
      std::string s_data = get_rand_astring(name_len);
      vector<std::string> s_dist;

      for (int s_d_itr = 0; s_d_itr < stock_dist_count; s_d_itr++)
        s_dist.push_back(std::to_string(s_d_itr) + s_data);

      record* stock_rec_ptr = new stock_record(stock_table_schema, s_i_itr,
                                               w_itr, s_quantity, s_dist, s_ytd,
                                               s_order_cnt, s_remote_cnt,
                                               s_data);

      log_str = serialize(stock_rec_ptr, stock_table_schema);
      //LOG_INFO("stock ::%s", log_str.c_str());

      st = statement(txn_id, operation_type::Insert, STOCK_TABLE_ID,
                     stock_rec_ptr);

      ee->load(st);
    }

    ee->txn_end(true);

  }
}

void tpcc_benchmark::load() {
  engine* ee = new engine(conf, tid, db, false);

  LOG_INFO("Load items ");
  load_items(ee);

  LOG_INFO("Load warehouses ");
  load_warehouses(ee);

  delete ee;
}

void tpcc_benchmark::do_delivery(engine* ee) {
  LOG_INFO("Delivery ");
  /*
   "getNewOrder": "SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID > -1 LIMIT 1", #
   "deleteNewOrder": "DELETE FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID = ?", # d_id, w_id, no_o_id
   "getCId": "SELECT O_C_ID FROM ORDERS WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?", # no_o_id, d_id, w_id
   "updateOrders": "UPDATE ORDERS SET O_CARRIER_ID = ? WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?", # o_carrier_id, no_o_id, d_id, w_id
   "updateOrderLine": "UPDATE ORDER_LINE SET OL_DELIVERY_D = ? WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?", # o_entry_d, no_o_id, d_id, w_id
   "sumOLAmount": "SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?", # no_o_id, d_id, w_id
   "updateCustomer": "UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + ? WHERE C_ID = ? AND C_D_ID = ? AND C_W_ID = ?", # ol_total, c_id, d_id, w_id
   */

  record* rec_ptr;
  statement st;
  vector<int> field_ids;
  std::string empty;

  txn_id++;
  TIMER(ee->txn_begin());

  int d_itr;

  int w_id = get_rand_int(0, warehouse_count);
  int o_carrier_id = get_rand_int(orders_min_carrier_id, orders_max_carrier_id);
  double ol_delivery_ts = static_cast<double>(time(NULL));
  std::string new_order_str, orders_str, order_line_str, customer_str;

  for (d_itr = 0; d_itr < districts_per_warehouse; d_itr++) {
    LOG_INFO("d_itr :: %d  w_id :: %d ", d_itr, w_id);

    // getNewOrder
    rec_ptr = new new_order_record(new_order_table_schema, 0, d_itr, w_id);

    st = statement(txn_id, operation_type::Select, NEW_ORDER_TABLE_ID, rec_ptr,
                   0, new_order_table_schema);

    TIMER(new_order_str = ee->select(st))

    if (new_order_str.empty()) {
      TIMER(ee->txn_end(false));
      return;
    }
    LOG_INFO("new_order :: %s", new_order_str.c_str());

    // deleteNewOrder
    rec_ptr = deserialize(new_order_str, new_order_table_schema);

    int o_id = std::stoi(rec_ptr->get_data(0));
    LOG_INFO("o_id :: %d ", o_id);

    st = statement(txn_id, operation_type::Delete, NEW_ORDER_TABLE_ID, rec_ptr);

    ee->remove(st);

    // getCId
    rec_ptr = new orders_record(orders_table_schema, o_id, 0, d_itr, w_id, 0, 0,
                                0, 0);

    st = statement(txn_id, operation_type::Select, ORDERS_TABLE_ID, rec_ptr, 0,
                   orders_table_schema);

    TIMER(orders_str = ee->select(st))

    if (orders_str.empty()) {
      TIMER(ee->txn_end(false));
      return;
    }
    LOG_INFO("orders :: %s ", orders_str.c_str());

    rec_ptr = deserialize(orders_str, orders_table_schema);

    int c_id = std::stoi(rec_ptr->get_data(1));

    LOG_INFO("c_id :: %d ", c_id);

    // updateOrders

    rec_ptr->set_int(5, o_carrier_id);
    field_ids = {5};  // carried id

    st = statement(txn_id, operation_type::Update, ORDERS_TABLE_ID, rec_ptr,
                   field_ids);

    TIMER(ee->update(st));

    // updateOrderLine
    double ol_ts = static_cast<double>(time(NULL));

    rec_ptr = new order_line_record(order_line_table_schema, o_id, d_itr, w_id,
                                    0, 0, 0, ol_ts, 0, 0, empty);

    rec_ptr->set_double(6, ol_delivery_ts);
    field_ids = {6};  // ol_ts

    st = statement(txn_id, operation_type::Update, ORDER_LINE_TABLE_ID, rec_ptr,
                   field_ids);

    TIMER(ee->update(st));

    //sumOLAmount
    rec_ptr = new order_line_record(order_line_table_schema, o_id, d_itr, w_id,
                                    0, 0, 0, 0, 0, 0, empty);

    st = statement(txn_id, operation_type::Select, ORDER_LINE_TABLE_ID, rec_ptr,
                   0, order_line_table_schema);

    TIMER(order_line_str = ee->select(st))

    if (order_line_str.empty()) {
      TIMER(ee->txn_end(false));
      return;
    }
    LOG_INFO("order_line :: %s ", order_line_str.c_str());

    rec_ptr = deserialize(order_line_str, order_line_table_schema);

    double ol_amount = std::stod(rec_ptr->get_data(8));
    LOG_INFO("ol_amount :: %.2lf ", ol_amount);

    // updateCustomer

    rec_ptr = new customer_record(customer_table_schema, c_id, d_itr, w_id,
                                  empty, empty, empty, empty, 0, 0, 0, 0, 0, 0,
                                  0, empty);

    st = statement(txn_id, operation_type::Update, CUSTOMER_TABLE_ID, rec_ptr,
                   0, customer_table_schema);

    TIMER(customer_str = ee->select(st))

    if (customer_str.empty()) {
      TIMER(ee->txn_end(false));
      return;
    }
    LOG_INFO("customer :: %s ", customer_str.c_str());

    rec_ptr = deserialize(customer_str, customer_table_schema);

    double orig_balance = std::stod(rec_ptr->get_data(16));  // balance
    LOG_INFO("orig_balance :: %.2lf ", orig_balance);

    field_ids = {16};  // ol_ts

    rec_ptr->set_double(16, orig_balance + ol_amount);

    st = statement(txn_id, operation_type::Update, CUSTOMER_TABLE_ID, rec_ptr,
                   field_ids);

    TIMER(ee->update(st));
  }

  TIMER(ee->txn_end(true));

}

void tpcc_benchmark::do_new_order(engine* ee, bool finish) {
  /*
   "NEW_ORDER": {
   "getWarehouseTaxRate": "SELECT W_TAX FROM WAREHOUSE WHERE W_ID = ?", # w_id
   "getDistrict": "SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = ? AND D_W_ID = ?", # d_id, w_id
   "getCustomer": "SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # w_id, d_id, c_id
   "incrementNextOrderId": "UPDATE DISTRICT SET D_NEXT_O_ID = ? WHERE D_ID = ? AND D_W_ID = ?", # d_next_o_id, d_id, w_id
   "createOrder": "INSERT INTO ORDERS (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", # d_next_o_id, d_id, w_id, c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local
   "createNewOrder": "INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES (?, ?, ?)", # o_id, d_id, w_id
   "getItemInfo": "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = ?", # ol_i_id
   "getStockInfo": "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_%02d FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?", # d_id, ol_i_id, ol_supply_w_id
   "updateStock": "UPDATE STOCK SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ? WHERE S_I_ID = ? AND S_W_ID = ?", # s_quantity, s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id
   "createOrderLine": "INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", # o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info
   },
   */

  LOG_INFO("New_Order ");

  record* rec_ptr;
  statement st;
  vector<int> field_ids;
  std::string empty;
  vector<std::string> empty_v(10);

  int w_id = get_rand_int(0, warehouse_count);
  int d_id = get_rand_int(0, districts_per_warehouse);
  int c_id = get_rand_int(0, customers_per_district);
  int o_ol_cnt = get_rand_int(orders_min_ol_cnt, orders_max_ol_cnt);
  double o_entry_ts = static_cast<double>(time(NULL));
  vector<int> i_ids, i_w_ids, i_qtys;
  int o_all_local = 1;
  std::string warehouse_str, district_str, customer_str, item_str, stock_str;

  for (int ol_itr = 0; ol_itr < o_ol_cnt; ol_itr++) {
    i_ids.push_back(get_rand_int(0, item_count));
    bool remote = get_rand_bool(0.01);
    i_w_ids.push_back(w_id);

    if (remote) {
      i_w_ids[ol_itr] = get_rand_int_excluding(0, warehouse_count, w_id);
      o_all_local = 0;
    }

    i_qtys.push_back(get_rand_int(0, order_line_max_ol_quantity));
  }

  txn_id++;
  TIMER(ee->txn_begin());

  // ----------------
  // Collect Information from WAREHOUSE, DISTRICT, and CUSTOMER
  // ----------------
  LOG_INFO("d_id ::  %d  w_id :: %d ", d_id, w_id);

  // getWarehouseTaxRate
  rec_ptr = new warehouse_record(warehouse_table_schema, w_id, empty, empty,
                                 empty, 0, 0);

  st = statement(txn_id, operation_type::Select, WAREHOUSE_TABLE_ID, rec_ptr, 0,
                 warehouse_table_schema);

  TIMER(warehouse_str = ee->select(st))

  if (warehouse_str.empty()) {
    TIMER(ee->txn_end(false));
    return;
  }
  LOG_INFO("warehouse ::  %s", warehouse_str.c_str());

  rec_ptr = deserialize(warehouse_str, warehouse_table_schema);

  double w_tax = std::stod(rec_ptr->get_data(7));

  LOG_INFO("w_tax :: %.2lf ", w_tax);

  // getDistrict
  rec_ptr = new district_record(district_table_schema, d_id, w_id, empty, empty,
                                empty, 0, 0, 0);

  st = statement(txn_id, operation_type::Select, DISTRICT_TABLE_ID, rec_ptr, 0,
                 district_table_schema);

  TIMER(district_str = ee->select(st))

  if (district_str.empty()) {
    TIMER(ee->txn_end(false));
    return;
  }
  LOG_INFO("district :: %s ", district_str.c_str());

  rec_ptr = deserialize(district_str, district_table_schema);

  double d_tax = std::stod(rec_ptr->get_data(8));
  int d_next_o_id = std::stoi(rec_ptr->get_data(10));
  int o_id = d_next_o_id;

  LOG_INFO("d_tax :: %.2lf ", d_tax);
  LOG_INFO("d_next_o_id ::  %d ", d_next_o_id);

  // ----------------
  // Collect Information from WAREHOUSE, DISTRICT, and CUSTOMER
  // ----------------

  // incrementNextOrderId

  rec_ptr->set_int(10, d_next_o_id + 1);

  field_ids = {10};

  st = statement(txn_id, operation_type::Update, DISTRICT_TABLE_ID, rec_ptr,
                 field_ids);

  TIMER(ee->update(st));

  // getCustomer
  rec_ptr = new customer_record(customer_table_schema, c_id, d_id, w_id, empty,
                                empty, empty, empty, 0, 0, 0, 0, 0, 0, 0,
                                empty);

  st = statement(txn_id, operation_type::Select, CUSTOMER_TABLE_ID, rec_ptr, 0,
                 customer_do_new_order_schema);

  TIMER(customer_str = ee->select(st))

  if (customer_str.empty()) {
    TIMER(ee->txn_end(false));
    return;
  }
  LOG_INFO("customer :: %s ", customer_str.c_str());

  rec_ptr = deserialize(customer_str, customer_table_schema);

  double c_discount = std::stod(rec_ptr->get_data(15));
  LOG_INFO("c_discount :: %.2lf ", c_discount);

  // createOrder

  int o_carrier_id = orders_null_carrier_id;

  rec_ptr = new orders_record(orders_table_schema, o_id, c_id, d_id, w_id,
                              o_entry_ts, o_carrier_id, o_ol_cnt, o_all_local);

  st = statement(txn_id, operation_type::Insert, ORDERS_TABLE_ID, rec_ptr);

  TIMER(ee->insert(st));

  // createNewOrder

  rec_ptr = new new_order_record(new_order_table_schema, o_id, d_id, w_id);

  st = statement(txn_id, operation_type::Insert, NEW_ORDER_TABLE_ID, rec_ptr);

  TIMER(ee->insert(st));

  // ----------------
  // Insert Order Item Information
  // ----------------
  // ITEMS
  int ol_total = 0;
  for (unsigned int i_itr = 0; i_itr < i_ids.size(); i_itr++) {

    int ol_i_id = i_ids[i_itr];
    int ol_supply_w_id = i_w_ids[i_itr];
    int ol_number = i_itr;
    int ol_quantity = i_qtys[i_itr];

    // getItemInfo
    rec_ptr = new item_record(item_table_schema, ol_i_id, 0, empty, 0);

    st = statement(txn_id, operation_type::Select, ITEM_TABLE_ID, rec_ptr, 0,
                   item_table_schema);

    TIMER(item_str = ee->select(st))

    if (item_str.empty()) {
      TIMER(ee->txn_end(false));
      return;
    }
    LOG_INFO("item :: %s ", item_str.c_str());

    rec_ptr = deserialize(item_str, item_table_schema);

    std::string i_name = rec_ptr->get_data(2);
    double i_price = std::stod(rec_ptr->get_data(3));

    // getStockInfo

    rec_ptr = new stock_record(stock_table_schema, ol_i_id, ol_supply_w_id, 0,
                               empty_v, 0, 0, 0, empty);

    st = statement(txn_id, operation_type::Select, STOCK_TABLE_ID, rec_ptr, 0,
                   stock_table_schema);

    TIMER(stock_str = ee->select(st))

    if (stock_str.empty()) {
      TIMER(ee->txn_end(false));
      return;
    }
    LOG_INFO("stock :: %s", stock_str.c_str());

    rec_ptr = deserialize(stock_str, stock_table_schema);

    int s_quantity = std::stoi(rec_ptr->get_data(2));
    int s_ytd = std::stoi(rec_ptr->get_data(13));
    int s_order_cnt = std::stoi(rec_ptr->get_data(14));
    int s_remote_cnt = std::stoi(rec_ptr->get_data(15));
    std::string s_ol_data = rec_ptr->get_data(16);

    // updateStock
    s_ytd += ol_quantity;

    if (s_quantity >= ol_quantity + 10)
      s_quantity = s_quantity - ol_quantity;
    else
      s_quantity = s_quantity + 91 - ol_quantity;
    s_order_cnt += 1;
    if (ol_supply_w_id != w_id)
      s_remote_cnt += 1;

    rec_ptr->set_int(2, s_quantity);
    rec_ptr->set_int(13, s_ytd);
    rec_ptr->set_int(14, s_order_cnt);
    rec_ptr->set_int(15, s_remote_cnt);

    field_ids = {2, 13, 14, 15};

    st = statement(txn_id, operation_type::Update, STOCK_TABLE_ID, rec_ptr,
                   field_ids);

    TIMER(ee->update(st));

    // createOrderLine

    int ol_amount = ol_quantity * i_price;

    rec_ptr = new order_line_record(order_line_table_schema, o_id, d_id, w_id,
                                    ol_number, ol_i_id, ol_supply_w_id,
                                    o_entry_ts, ol_quantity, ol_amount,
                                    s_ol_data);

    st = statement(txn_id, operation_type::Insert, ORDER_LINE_TABLE_ID,
                   rec_ptr);

    TIMER(ee->insert(st));

    ol_total += ol_amount;
  }

  ol_total *= (1 - c_discount) * (1 + w_tax + d_tax);

  if (ol_total > 0)
    LOG_INFO("ol_total :: %d ", ol_total);

  if (finish)
    TIMER(ee->txn_end(true));

}

void tpcc_benchmark::do_order_status(engine* ee) {
  /*
   "ORDER_STATUS": {
   "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # w_id, d_id, c_id
   "getCustomersByLastName": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST", # w_id, d_id, c_last
   "getLastOrder": "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1", # w_id, d_id, c_id
   "getOrderLines": "SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?", # w_id, d_id, o_id
   },
   */

  LOG_INFO("Order_Status ");

  record* rec_ptr;
  statement st;
  vector<int> field_ids;
  std::string empty;
  vector<std::string> empty_v(10);

  txn_id++;
  TIMER(ee->txn_begin());

  unsigned int d_itr = 0;

  int w_id = get_rand_int(0, warehouse_count);
  int d_id = get_rand_int(0, districts_per_warehouse);
  int c_id = get_rand_int(0, customers_per_district);
  std::string c_name = get_rand_astring(name_len);
  bool lookup_by_name = get_rand_bool(0.8);
  std::string customer_str, orders_str, order_line_str;

  if (lookup_by_name) {
    // getCustomerByCustomerId
    rec_ptr = new customer_record(customer_table_schema, c_id, d_id, w_id,
                                  empty, empty, empty, empty, 0, 0, 0, 0, 0, 0,
                                  0, empty);

    st = statement(txn_id, operation_type::Select, CUSTOMER_TABLE_ID, rec_ptr,
                   0, customer_table_schema);

    TIMER(customer_str = ee->select(st))

    if (customer_str.empty()) {
      TIMER(ee->txn_end(false));
      return;
    }
    LOG_INFO("customer :: %s ", customer_str.c_str());
  } else {
// getCustomerByLastName
    rec_ptr = new customer_record(customer_table_schema, 0, d_id, w_id, c_name,
                                  empty, empty, empty, 0, 0, 0, 0, 0, 0, 0,
                                  empty);

    st = statement(txn_id, operation_type::Select, CUSTOMER_TABLE_ID, rec_ptr,
                   1, customer_table_schema);

    TIMER(customer_str = ee->select(st))

    if (customer_str.empty()) {
      TIMER(ee->txn_end(false));
      return;
    }
    LOG_INFO("customer by name :: %s ", customer_str.c_str());

    rec_ptr = deserialize(customer_str, customer_table_schema);

    c_id = std::stoi(rec_ptr->get_data(0));

    LOG_INFO("c_id :: %d", c_id);
  }

// getLastOrder
  rec_ptr = new orders_record(orders_table_schema, 0, c_id, d_itr, w_id, 0, 0,
                              0, 0);

  st = statement(txn_id, operation_type::Select, ORDERS_TABLE_ID, rec_ptr, 1,
                 orders_table_schema);

  TIMER(orders_str = ee->select(st)
  ;
  )

  if (orders_str.empty()) {
    TIMER(ee->txn_end(false));
    return;
  }

  LOG_INFO("orders :: %s ", orders_str.c_str());

  rec_ptr = deserialize(orders_str, orders_table_schema);

  c_id = std::stoi(rec_ptr->get_data(0));

  LOG_INFO("c_id :: %d ", c_id);

// getOrderLines
  rec_ptr = new order_line_record(order_line_table_schema, c_id, d_itr, w_id, 0,
                                  0, 0, 0, 0, 0, empty);

  st = statement(txn_id, operation_type::Select, ORDER_LINE_TABLE_ID, rec_ptr,
                 1, order_line_table_schema);

  TIMER(order_line_str = ee->select(st)
  ;
  )

  if (order_line_str.empty()) {
    TIMER(ee->txn_end(false));
    return;
  }

  LOG_INFO("order_line :: %s", order_line_str.c_str());

  TIMER(ee->txn_end(true));

}

void tpcc_benchmark::do_payment(engine* ee) {

  /*
   "getWarehouse": "SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM WAREHOUSE WHERE W_ID = ?", # w_id
   "updateWarehouseBalance": "UPDATE WAREHOUSE SET W_YTD = W_YTD + ? WHERE W_ID = ?", # h_amount, w_id
   "getDistrict": "SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?", # w_id, d_id
   "updateDistrictBalance": "UPDATE DISTRICT SET D_YTD = D_YTD + ? WHERE D_W_ID = ? AND D_ID = ?", # h_amount, d_w_id, d_id
   "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # w_id, d_id, c_id
   "getCustomersByLastName": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST", # w_id, d_id, c_last
   "updateBCCustomer": "UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ?, C_DATA = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # c_balance, c_ytd_payment, c_payment_cnt, c_data, c_w_id, c_d_id, c_id
   "updateGCCustomer": "UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # c_balance, c_ytd_payment, c_payment_cnt, c_w_id, c_d_id, c_id
   "insertHistory": "INSERT INTO HISTORY VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
   */

  LOG_INFO("Payment ");

  record* rec_ptr;
  statement st;
  vector<int> field_ids;
  std::string empty;

  bool pay_local = get_rand_bool(0.85);
  bool pay_by_name = get_rand_bool(0.10);  // 0.60
  int w_id = get_rand_int(0, warehouse_count);
  int d_id = get_rand_int(0, districts_per_warehouse);
  double h_amount = get_rand_double(payment_min_amount, payment_max_amount);
  double h_ts = static_cast<double>(time(NULL));
  int c_w_id, c_d_id, c_id;
  std::string c_name;
  std::string customer_str;

  if (pay_local) {
    c_w_id = w_id;
    c_d_id = d_id;
  } else {
    c_w_id = get_rand_int_excluding(0, warehouse_count, w_id);
    c_d_id = d_id;
  }

  if (pay_by_name)
    c_name = get_rand_astring(name_len);
  else
    c_id = get_rand_int(0, customers_per_district);

  txn_id++;
  TIMER(ee->txn_begin());

  if (!pay_by_name) {
// getCustomerByCustomerId
    rec_ptr = new customer_record(customer_table_schema, c_id, d_id, w_id,
                                  empty, empty, empty, empty, 0, 0, 0, 0, 0, 0,
                                  0, empty);

    st = statement(txn_id, operation_type::Select, CUSTOMER_TABLE_ID, rec_ptr,
                   0, customer_table_schema);

    TIMER(customer_str = ee->select(st))

    if (customer_str.empty()) {
      TIMER(ee->txn_end(false));
      return;
    }
    LOG_INFO("customer :: %s ", customer_str.c_str());
  } else {
// getCustomerByLastName
    rec_ptr = new customer_record(customer_table_schema, 0, d_id, w_id, c_name,
                                  empty, empty, empty, 0, 0, 0, 0, 0, 0, 0,
                                  empty);

    st = statement(txn_id, operation_type::Select, CUSTOMER_TABLE_ID, rec_ptr,
                   1, customer_table_schema);

    TIMER(customer_str = ee->select(st))

    if (customer_str.empty()) {
      TIMER(ee->txn_end(false));
      return;
    }
    LOG_INFO("customer by name :: %s", customer_str.c_str());

    rec_ptr = deserialize(customer_str, customer_table_schema);

    c_id = std::stoi(rec_ptr->get_data(0));

    LOG_INFO("c_id :: %d ", c_id);
  }

  rec_ptr = deserialize(customer_str, customer_table_schema);

  int c_balance = std::stoi(rec_ptr->get_data(16));
  int c_ytd_payment = std::stoi(rec_ptr->get_data(17));
  int c_payment_cnt = std::stoi(rec_ptr->get_data(18));
  std::string c_data = rec_ptr->get_data(20);
  std::string c_credit = rec_ptr->get_data(13);
  std::string warehouse_str, district_str;

  c_balance -= h_amount;
  c_ytd_payment += h_amount;
  c_payment_cnt += 1;

  LOG_INFO("c_balance :: %d ", c_balance);

// getWarehouse
  rec_ptr = new warehouse_record(warehouse_table_schema, w_id, empty, empty,
                                 empty, 0, 0);

  st = statement(txn_id, operation_type::Select, WAREHOUSE_TABLE_ID, rec_ptr, 0,
                 warehouse_table_schema);

  TIMER(warehouse_str = ee->select(st))

  if (warehouse_str.empty()) {
    TIMER(ee->txn_end(false));
    return;
  }
  LOG_INFO("warehouse :: %s ", warehouse_str.c_str());

  rec_ptr = deserialize(warehouse_str, warehouse_table_schema);

  double w_ytd = std::stod(rec_ptr->get_data(8));

  LOG_INFO("w_ytd :: %.2lf ", w_ytd);

// updateWarehouseBalance

  w_ytd += h_amount;

  rec_ptr->set_double(8, w_ytd);

  field_ids = {8};  // w_ytd

  st = statement(txn_id, operation_type::Update, WAREHOUSE_TABLE_ID, rec_ptr,
                 field_ids);

  TIMER(ee->update(st));

// getDistrict
  rec_ptr = new district_record(district_table_schema, d_id, w_id, empty, empty,
                                empty, 0, 0, 0);

  st = statement(txn_id, operation_type::Select, DISTRICT_TABLE_ID, rec_ptr, 0,
                 district_table_schema);

  TIMER(district_str = ee->select(st))

  if (district_str.empty()) {
    TIMER(ee->txn_end(false));
    return;
  }

  LOG_INFO("district :: %s ", district_str.c_str());

  rec_ptr = deserialize(district_str, district_table_schema);

  double d_ytd = std::stod(rec_ptr->get_data(9));

  LOG_INFO("d_ytd :: %.2lf ", d_ytd);

// updateDistrictBalance

  d_ytd += h_amount;

  rec_ptr->set_double(9, d_ytd);

  field_ids = {9};  // w_ytd

  st = statement(txn_id, operation_type::Update, DISTRICT_TABLE_ID, rec_ptr,
                 field_ids);

  TIMER(ee->update(st));

  // Customer Credit Information
  LOG_INFO("c_credit :: %s ", c_credit.c_str());

  if (c_credit == customers_bcredit + " ") {
// updateBCCustomer

    c_data = std::string(
        std::to_string(c_id) + " " + std::to_string(d_id) + " "
            + std::to_string(w_id) + " " + std::to_string(c_d_id) + " "
            + std::to_string(c_w_id) + " " + std::to_string(h_amount));

    rec_ptr = new customer_record(customer_table_schema, c_id, d_id, w_id,
                                  empty, empty, empty, empty, 0, 0, 0,
                                  c_balance, c_ytd_payment, c_payment_cnt, 0,
                                  c_data);

    field_ids = {16, 17, 18, 20};

    st = statement(txn_id, operation_type::Update, CUSTOMER_TABLE_ID, rec_ptr,
                   field_ids);

    TIMER(ee->update(st));

  } else {

// updateGCCustomer

    rec_ptr = new customer_record(customer_table_schema, c_id, d_id, w_id,
                                  empty, empty, empty, empty, 0, 0, 0,
                                  c_balance, c_ytd_payment, c_payment_cnt, 0,
                                  empty);

    field_ids = {16, 17, 18};

    st = statement(txn_id, operation_type::Update, CUSTOMER_TABLE_ID, rec_ptr,
                   field_ids);

    TIMER(ee->update(st));

  }

// insertHistory

  std::string h_data = std::to_string(w_id) + "    " + std::to_string(d_id);

  rec_ptr = new history_record(history_table_schema, c_id, c_d_id, c_w_id, d_id,
                               w_id, h_ts, h_amount, h_data);

  st = statement(txn_id, operation_type::Insert, HISTORY_TABLE_ID, rec_ptr);

  TIMER(ee->insert(st));

  /* TPC-C 2.5.3.3: Must display the following fields:
   W_ID, D_ID, C_ID, C_D_ID, C_W_ID, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP,
   D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1,
   C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM,
   C_DISCOUNT, C_BALANCE, the first 200 characters of C_DATA (only if C_CREDIT = "BC"),
   H_AMOUNT, and H_DATE.
   */

  // Hand back all the warehouse, district, and customer data
  std::string payment_str = warehouse_str + district_str + customer_str;

  TIMER(ee->txn_end(true));

}

void tpcc_benchmark::do_stock_level(engine* ee) {

  /*
   "getOId": "SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?",
   "getStockCount": "SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK  WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID < ? AND OL_O_ID >= ? AND S_W_ID = ? AND S_I_ID = OL_I_ID AND S_QUANTITY < ?
   */

  LOG_INFO("Stock Level ");

  record* rec_ptr;
  statement st;
  vector<int> field_ids;
  std::string empty;
  vector<std::string> empty_v(10);

  int w_id = get_rand_int(0, warehouse_count);
  int d_id = get_rand_int(0, districts_per_warehouse);
  int threshold = get_rand_int(stock_min_threshold, stock_max_threshold);
  std::string district_str, order_line_str, stock_str;

  txn_id++;
  TIMER(ee->txn_begin());

// getOId
  rec_ptr = new district_record(district_table_schema, d_id, w_id, empty, empty,
                                empty, 0, 0, 0);

  st = statement(txn_id, operation_type::Select, DISTRICT_TABLE_ID, rec_ptr, 0,
                 district_table_schema);

  TIMER(district_str = ee->select(st))

  if (district_str.empty()) {
    TIMER(ee->txn_end(false));
    return;
  }
  LOG_INFO("district :: %s ", district_str.c_str());

  rec_ptr = deserialize(district_str, district_table_schema);

  int d_next_o_id = std::stoi(rec_ptr->get_data(10));

  LOG_INFO("d_next_o_id :: %d ", d_next_o_id);

// getStockCount
  std::set<int> items;
  int min_o_id = std::max(0, d_next_o_id - 20);

  for (int o_id = min_o_id; o_id < d_next_o_id; o_id++) {
    LOG_INFO("o_id :: %d ", o_id);

    rec_ptr = new order_line_record(order_line_table_schema, o_id, d_id, w_id,
                                    0, 0, 0, 0, 0, 0, empty);

    st = statement(txn_id, operation_type::Select, ORDER_LINE_TABLE_ID, rec_ptr,
                   1, order_line_table_schema);

    TIMER(order_line_str = ee->select(st))

    if (order_line_str.empty())
      break;

    LOG_INFO("order_line :: %s ", order_line_str.c_str());

    rec_ptr = deserialize(order_line_str, order_line_table_schema);

    int s_i_id = std::stoi(rec_ptr->get_data(4));

    LOG_INFO("s_i_id :: %d ", s_i_id);

    rec_ptr = new stock_record(stock_table_schema, o_id, w_id, 0, empty_v, 0, 0,
                               0, empty);

    st = statement(txn_id, operation_type::Select, STOCK_TABLE_ID, rec_ptr, 0,
                   stock_table_do_stock_level_schema);

    TIMER(stock_str = ee->select(st))

    if (stock_str.empty()) {
      TIMER(ee->txn_end(false));
      return;
    }
    LOG_INFO("stock :: %s ", stock_str.c_str());

    rec_ptr = deserialize(stock_str, stock_table_schema);

    int s_quantity = std::stoi(rec_ptr->get_data(2));

    LOG_INFO("s_quantity :: %d ", s_quantity);

    if (s_quantity < threshold) {
      items.insert(s_i_id);
    }
  }

  int i_count = items.size();
  LOG_INFO("i_count :: %d ", i_count);

  TIMER(ee->txn_end(true));
}

void tpcc_benchmark::sim_crash() {
  engine* ee = new engine(conf, tid, db, false);

  // NEW ORDER
  do_new_order(ee, false);

  delete ee;
}

void tpcc_benchmark::execute() {
  engine* ee = new engine(conf, tid, db, false);
  unsigned int txn_itr;
  status ss(num_txns);

  for (txn_itr = 0; txn_itr < num_txns; txn_itr++) {
    double u = uniform_dist[txn_itr];

    if (conf.tpcc_stock_level_only) {
      do_stock_level(ee);
    } else {

      if (u <= 0.04) {
        //cout << "stock_level " << endl;
        do_stock_level(ee);
      } else if (u <= 0.08) {
        //cout << "delivery " << endl;
        do_delivery(ee);
      } else if (u <= 0.12) {
        //cout << "order_status " << endl;
        do_order_status(ee);
      } else if (u <= 0.55) {
        //cout << "payment " << endl;
        do_payment(ee);
      } else {
        //cout << "new_order " << endl;
        do_new_order(ee);
      }
    }

    if (tid == 0)
      ss.display();
  }

  delete ee;
}
