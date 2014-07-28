// TPCC BENCHMARK

#include "common/tpcc_benchmark.h"

#include <sys/types.h>
#include <ctime>
#include <iostream>
#include <string>

#include "common/field.h"
#include "common/libpm.h"
#include "common/plist.h"
#include "common/schema.h"
#include "common/table.h"
#include "common/table_index.h"

using namespace std;

tpcc_benchmark::tpcc_benchmark(config& _conf)
    : conf(_conf),
      txn_id(0) {

  btype = benchmark_type::TPCC;

  // Initialization mode
  if (conf.sp->init == 0) {
    //cout << "Initialization Mode" << endl;

    database* db = new database(conf);
    conf.sp->ptrs[0] = db;
    pmemalloc_activate(db);
    conf.db = db;

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

    conf.sp->init = 1;
  } else {
    //cout << "Recovery Mode " << endl;
    database* db = (database*) conf.sp->ptrs[0];
    db->reset(conf);
    conf.db = db;
  }

  uniform(uniform_dist, conf.num_txns);
}

// WAREHOUSE
class warehouse_record : public record {
 public:
  warehouse_record(schema* sptr, int w_id, const std::string& w_name,
                   const std::string& w_st, const std::string& w_zip,
                   const double w_tax, const double w_ytd)

      : record(sptr) {

    char* vc = NULL;
    std::sprintf(&(data[sptr->columns[0].offset]), "%d", w_id);

    for (int itr = 1; itr <= 4; itr++) {
      vc = new char[w_name.size() + 1];
      strcpy(vc, w_name.c_str());
      std::sprintf(&(data[sptr->columns[itr].offset]), "%p", vc);
    }

    vc = new char[w_st.size() + 1];
    strcpy(vc, w_st.c_str());
    std::sprintf(&(data[sptr->columns[5].offset]), "%p", vc);

    vc = new char[w_zip.size() + 1];
    strcpy(vc, w_zip.c_str());
    std::sprintf(&(data[sptr->columns[6].offset]), "%p", vc);

    std::sprintf(&(data[sptr->columns[7].offset]), "%.2f", w_tax);
    std::sprintf(&(data[sptr->columns[8].offset]), "%.2f", w_ytd);
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
    field = field_info(offset, 12, 16, field_type::VARCHAR, 0, 1);
    offset += field.ser_len;
    cols.push_back(field);
  }

  field = field_info(offset, 12, 2, field_type::VARCHAR, 0, 1);
  offset += field.ser_len;
  cols.push_back(field);
  field = field_info(offset, 12, 9, field_type::VARCHAR, 0, 1);
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

  table* warehouse = new table("warehouse", warehouse_schema, 1, conf);
  pmemalloc_activate(warehouse);

  // PRIMARY INDEX
  for (int itr = 1; itr <= cols.size(); itr++) {
    cols[itr].enabled = 0;
  }

  schema* warehouse_index_schema = new schema(cols);
  pmemalloc_activate(warehouse_index_schema);

  table_index* key_index = new table_index(warehouse_index_schema, cols.size(),
                                           conf);
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

    char* vc = NULL;
    std::sprintf(&(data[sptr->columns[0].offset]), "%d", d_id);
    std::sprintf(&(data[sptr->columns[1].offset]), "%d", d_w_id);

    for (int itr = 2; itr <= 5; itr++) {
      vc = new char[d_name.size() + 1];
      strcpy(vc, d_name.c_str());
      std::sprintf(&(data[sptr->columns[itr].offset]), "%p", vc);
    }

    vc = new char[d_st.size() + 1];
    strcpy(vc, d_st.c_str());
    std::sprintf(&(data[sptr->columns[6].offset]), "%p", vc);

    vc = new char[d_zip.size() + 1];
    strcpy(vc, d_zip.c_str());
    std::sprintf(&(data[sptr->columns[7].offset]), "%p", vc);

    std::sprintf(&(data[sptr->columns[8].offset]), "%.2f", d_tax);
    std::sprintf(&(data[sptr->columns[9].offset]), "%.2f", d_ytd);
    std::sprintf(&(data[sptr->columns[10].offset]), "%d", d_next_o_id);
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
    field = field_info(offset, 12, 16, field_type::VARCHAR, 0, 1);
    offset += field.ser_len;
    cols.push_back(field);
  }
  for (int f_itr = 6; f_itr <= 7; f_itr++) {
    field = field_info(offset, 12, 2, field_type::VARCHAR, 0, 1);
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

  table* district = new table("district", district_schema, 1, conf);
  pmemalloc_activate(district);

  // PRIMARY INDEX
  for (int itr = 2; itr <= cols.size(); itr++) {
    cols[itr].enabled = 0;
  }

  schema* district_index_schema = new schema(cols);
  pmemalloc_activate(district_index_schema);

  table_index* key_index = new table_index(district_index_schema, cols.size(),
                                           conf);
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

    char* vc = NULL;
    std::sprintf(&(data[sptr->columns[0].offset]), "%d", i_id);
    std::sprintf(&(data[sptr->columns[1].offset]), "%d", i_im_id);

    vc = new char[i_name.size() + 1];
    strcpy(vc, i_name.c_str());
    std::sprintf(&(data[sptr->columns[2].offset]), "%p", vc);

    std::sprintf(&(data[sptr->columns[3].offset]), "%.2f", i_price);

    vc = new char[i_name.size() + 1];
    strcpy(vc, i_name.c_str());
    std::sprintf(&(data[sptr->columns[4].offset]), "%p", vc);
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
  field = field_info(offset, 12, 32, field_type::VARCHAR, 0, 1);
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

  table* item = new table("item", item_schema, 1, conf);
  pmemalloc_activate(item);

  // PRIMARY INDEX
  for (int itr = 1; itr <= cols.size(); itr++) {
    cols[itr].enabled = 0;
  }

  schema* item_index_schema = new schema(cols);
  pmemalloc_activate(item_index_schema);

  table_index* key_index = new table_index(item_index_schema, cols.size(),
                                           conf);
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
                  const int c_delivery_cnt)
      : record(sptr) {

    char* vc = NULL;
    std::sprintf(&(data[sptr->columns[0].offset]), "%d", c_id);
    std::sprintf(&(data[sptr->columns[1].offset]), "%d", c_d_id);
    std::sprintf(&(data[sptr->columns[2].offset]), "%d", c_w_id);

    for (int itr = 3; itr <= 8; itr++) {
      vc = new char[c_name.size() + 1];
      strcpy(vc, c_name.c_str());
      std::sprintf(&(data[sptr->columns[itr].offset]), "%p", vc);
    }

    vc = new char[c_st.size() + 1];
    strcpy(vc, c_st.c_str());
    std::sprintf(&(data[sptr->columns[9].offset]), "%p", vc);

    vc = new char[c_zip.size() + 1];
    strcpy(vc, c_zip.c_str());
    std::sprintf(&(data[sptr->columns[10].offset]), "%p", vc);

    vc = new char[c_name.size() + 1];
    strcpy(vc, c_name.c_str());
    std::sprintf(&(data[sptr->columns[11].offset]), "%p", vc);

    std::sprintf(&(data[sptr->columns[12].offset]), "%.2f", c_ts);

    vc = new char[c_credit.size() + 1];
    strcpy(vc, c_credit.c_str());
    std::sprintf(&(data[sptr->columns[13].offset]), "%p", vc);

    std::sprintf(&(data[sptr->columns[14].offset]), "%.2f", c_credit_lim);
    std::sprintf(&(data[sptr->columns[15].offset]), "%.2f", c_discount);

    std::sprintf(&(data[sptr->columns[16].offset]), "%.2f", c_balance);
    std::sprintf(&(data[sptr->columns[17].offset]), "%.2f", c_ytd);
    std::sprintf(&(data[sptr->columns[18].offset]), "%d", c_payment_cnt);
    std::sprintf(&(data[sptr->columns[19].offset]), "%d", c_delivery_cnt);

    vc = new char[c_name.size() + 1];
    strcpy(vc, c_name.c_str());
    std::sprintf(&(data[sptr->columns[20].offset]), "%p", vc);
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
    field = field_info(offset, 12, 16, field_type::VARCHAR, 0, 1);
    offset += field.ser_len;
    cols.push_back(field);
  }

  field = field_info(offset, 12, 2, field_type::VARCHAR, 0, 1);
  offset += field.ser_len;
  cols.push_back(field);
  field = field_info(offset, 12, 9, field_type::VARCHAR, 0, 1);
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

  table* customer = new table("custome", customer_schema, 1, conf);
  pmemalloc_activate(customer);

  // PRIMARY INDEX
  for (int itr = 3; itr <= cols.size(); itr++) {
    cols[itr].enabled = 0;
  }

  schema* customer_index_schema = new schema(cols);
  pmemalloc_activate(customer_index_schema);

  table_index* p_index = new table_index(customer_index_schema, cols.size(),
                                         conf);
  pmemalloc_activate(p_index);
  customer->indices->push_back(p_index);

  // SECONDARY INDEX
  cols[2].enabled = 1;
  cols[3].enabled = 0;

  schema* customer_name_index_schema = new schema(cols);
  pmemalloc_activate(customer_name_index_schema);

  table_index* s_index = new table_index(customer_name_index_schema,
                                         cols.size(), conf);
  pmemalloc_activate(s_index);
  customer->indices->push_back(s_index);

  return customer;
}

// HISTORY

class history_record : public record {
 public:
  history_record(schema* sptr, int h_c_id, int h_c_d_id, int h_c_w_id,
                 int h_d_id, int h_w_id, const double h_ts,
                 const double h_amount, const std::string& h_data)
      : record(sptr) {

    char* vc = NULL;
    std::sprintf(&(data[sptr->columns[0].offset]), "%d", h_c_id);
    std::sprintf(&(data[sptr->columns[1].offset]), "%d", h_c_d_id);
    std::sprintf(&(data[sptr->columns[2].offset]), "%d", h_c_w_id);
    std::sprintf(&(data[sptr->columns[3].offset]), "%d", h_d_id);
    std::sprintf(&(data[sptr->columns[4].offset]), "%d", h_w_id);

    std::sprintf(&(data[sptr->columns[5].offset]), "%.2f", h_ts);
    std::sprintf(&(data[sptr->columns[6].offset]), "%.2f", h_amount);

    vc = new char[h_data.size() + 1];
    strcpy(vc, h_data.c_str());
    std::sprintf(&(data[sptr->columns[7].offset]), "%p", vc);
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

  table* history = new table("history", history_schema, 1, conf);
  pmemalloc_activate(history);

  // PRIMARY INDEX
  for (int itr = 5; itr <= cols.size(); itr++) {
    cols[itr].enabled = 0;
  }

  schema* history_index_schema = new schema(cols);
  pmemalloc_activate(history_index_schema);

  table_index* p_index = new table_index(history_index_schema, cols.size(),
                                         conf);
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

    char* vc = NULL;
    std::sprintf(&(data[sptr->columns[0].offset]), "%d", s_i_id);
    std::sprintf(&(data[sptr->columns[1].offset]), "%d", s_w_id);
    std::sprintf(&(data[sptr->columns[2].offset]), "%d", s_quantity);

    for (int f_itr = 3; f_itr <= 12; f_itr++) {
      int s_dist_itr = f_itr - 3;
      vc = new char[s_dist[s_dist_itr].size() + 1];
      strcpy(vc, s_dist[s_dist_itr].c_str());
      std::sprintf(&(data[sptr->columns[f_itr].offset]), "%p", vc);
    }

    std::sprintf(&(data[sptr->columns[13].offset]), "%d", s_ytd);
    std::sprintf(&(data[sptr->columns[14].offset]), "%d", s_order_cnt);
    std::sprintf(&(data[sptr->columns[15].offset]), "%d", s_remote_cnt);

    vc = new char[s_data.size() + 1];
    strcpy(vc, s_data.c_str());
    std::sprintf(&(data[sptr->columns[16].offset]), "%p", vc);
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

  table* stock = new table("stock", stock_schema, 1, conf);
  pmemalloc_activate(stock);

  // PRIMARY INDEX
  for (int itr = 3; itr <= cols.size(); itr++) {
    cols[itr].enabled = 0;
  }

  schema* stock_index_schema = new schema(cols);
  pmemalloc_activate(stock_index_schema);

  table_index* p_index = new table_index(stock_index_schema, cols.size(), conf);
  pmemalloc_activate(p_index);
  stock->indices->push_back(p_index);

  return stock;
}

// ORDERS

class orders_record : public record {
 public:
  orders_record(schema* sptr, int o_id, int o_c_id, int o_d_id, int o_w_id,
                double o_entry_ts, int o_carrier_id, int o_ol_cnt,
                int o_all_local)
      : record(sptr) {

    std::sprintf(&(data[sptr->columns[0].offset]), "%d", o_id);
    std::sprintf(&(data[sptr->columns[1].offset]), "%d", o_c_id);
    std::sprintf(&(data[sptr->columns[2].offset]), "%d", o_d_id);
    std::sprintf(&(data[sptr->columns[3].offset]), "%d", o_w_id);

    std::sprintf(&(data[sptr->columns[4].offset]), "%.2lf", o_entry_ts);

    std::sprintf(&(data[sptr->columns[5].offset]), "%d", o_carrier_id);
    std::sprintf(&(data[sptr->columns[6].offset]), "%d", o_ol_cnt);
    std::sprintf(&(data[sptr->columns[7].offset]), "%d", o_all_local);
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

  table* orders = new table("orders", orders_schema, 1, conf);
  pmemalloc_activate(orders);

  // PRIMARY INDEX
  for (int itr = 4; itr <= cols.size(); itr++) {
    cols[itr].enabled = 0;
  }
  cols[1].enabled = 0;

  schema* p_index_schema = new schema(cols);
  pmemalloc_activate(p_index_schema);

  table_index* p_index = new table_index(p_index_schema, cols.size(), conf);
  pmemalloc_activate(p_index);
  orders->indices->push_back(p_index);

  // SECONDARY INDEX
  cols[0].enabled = 0;
  cols[1].enabled = 1;

  schema* s_index_schema = new schema(cols);
  pmemalloc_activate(s_index_schema);

  table_index* s_index = new table_index(s_index_schema, cols.size(), conf);
  pmemalloc_activate(s_index);
  orders->indices->push_back(s_index);

  return orders;
}

// NEW_ORDER

class new_order_record : public record {
 public:
  new_order_record(schema* sptr, int no_o_id, int no_d_id, int no_w_id)
      : record(sptr) {

    std::sprintf(&(data[sptr->columns[0].offset]), "%d", no_o_id);
    std::sprintf(&(data[sptr->columns[1].offset]), "%d", no_d_id);
    std::sprintf(&(data[sptr->columns[2].offset]), "%d", no_w_id);
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

  table* new_order = new table("new_order", new_order_schema, 1, conf);
  pmemalloc_activate(new_order);

  // PRIMARY INDEX
  // XXX alter pkey
  cols[0].enabled = 0;

  schema* new_order_index_schema = new schema(cols);
  pmemalloc_activate(new_order_index_schema);

  table_index* new_order_index = new table_index(new_order_index_schema,
                                                 cols.size(), conf);
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

    char* vc = NULL;
    std::sprintf(&(data[sptr->columns[0].offset]), "%d", ol_o_id);
    std::sprintf(&(data[sptr->columns[1].offset]), "%d", ol_d_id);
    std::sprintf(&(data[sptr->columns[2].offset]), "%d", ol_w_id);
    std::sprintf(&(data[sptr->columns[3].offset]), "%d", ol_number);

    std::sprintf(&(data[sptr->columns[4].offset]), "%d", ol_i_id);
    std::sprintf(&(data[sptr->columns[5].offset]), "%d", ol_supply_w_id);

    std::sprintf(&(data[sptr->columns[6].offset]), "%.2lf", ol_delivery_ts);
    std::sprintf(&(data[sptr->columns[7].offset]), "%d", ol_quantity);
    std::sprintf(&(data[sptr->columns[8].offset]), "%.2lf", ol_amount);

    vc = new char[ol_info.size() + 1];
    strcpy(vc, ol_info.c_str());
    std::sprintf(&(data[sptr->columns[9].offset]), "%p", vc);
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

  table* order_line = new table("order_line", order_line_schema, 1, conf);
  pmemalloc_activate(order_line);

  // PRIMARY INDEX
  for (int itr = 4; itr <= cols.size(); itr++) {
    cols[itr].enabled = 0;
  }

  schema* p_index_schema = new schema(cols);
  pmemalloc_activate(p_index_schema);

  table_index* p_index = new table_index(p_index_schema, cols.size(), conf);
  pmemalloc_activate(p_index);
  order_line->indices->push_back(p_index);

  // SECONDARY INDEX
  cols[3].enabled = 0;

  schema* s_index_schema = new schema(cols);
  pmemalloc_activate(s_index_schema);

  table_index* s_index = new table_index(s_index_schema, cols.size(), conf);
  pmemalloc_activate(s_index);
  order_line->indices->push_back(s_index);

  return order_line;
}

void tpcc_benchmark::load_items(engine* ee) {
  int num_items = item_count;  //100000

  schema* item_table_schema = conf.db->tables->at(ITEM_TABLE_ID)->sptr;
  double tax = 0.1f;
  double ytd = 30000.00f;  // different from warehouse
  int next_d_o_id = 3000;
  unsigned int i_itr;

  for (i_itr = 0; i_itr < num_items; i_itr++) {

    txn_id++;
    ee->txn_begin();

    int i_im_id = i_itr * 10;
    std::string name = get_rand_astring(item_name_len);
    double price = get_rand_double(item_min_price, item_max_price);

    record* rec_ptr = new item_record(item_table_schema, i_itr, i_im_id, name,
                                      price);

    std::string key_str = get_data(rec_ptr, item_table_schema);
    cout << "item ::" << key_str << endl;

    statement st(txn_id, operation_type::Insert, ITEM_TABLE_ID, rec_ptr);

    ee->insert(st);

    ee->txn_end(true);
  }

}

void tpcc_benchmark::load_warehouses(engine* ee) {
  int num_warehouses = warehouse_count;

  schema* warehouse_table_schema = conf.db->tables->at(WAREHOUSE_TABLE_ID)->sptr;
  schema* district_table_schema = conf.db->tables->at(DISTRICT_TABLE_ID)->sptr;
  schema* customer_table_schema = conf.db->tables->at(CUSTOMER_TABLE_ID)->sptr;
  schema* history_table_schema = conf.db->tables->at(HISTORY_TABLE_ID)->sptr;
  schema* orders_table_schema = conf.db->tables->at(ORDERS_TABLE_ID)->sptr;
  schema* order_line_table_schema = conf.db->tables->at(ORDER_LINE_TABLE_ID)
      ->sptr;
  schema* new_order_table_schema = conf.db->tables->at(NEW_ORDER_TABLE_ID)->sptr;
  schema* stock_table_schema = conf.db->tables->at(STOCK_TABLE_ID)->sptr;

  unsigned int w_itr, d_itr, c_itr, o_itr, ol_itr, s_i_itr;
  statement st;
  std::string log_str;

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

    log_str = get_data(warehouse_rec_ptr, warehouse_table_schema);
    cout << "warehouse ::" << log_str << endl;

    st = statement(txn_id, operation_type::Insert, WAREHOUSE_TABLE_ID,
                   warehouse_rec_ptr);

    ee->insert(st);
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

      log_str = get_data(district_rec_ptr, district_table_schema);
      cout << "district ::" << log_str << endl;

      st = statement(txn_id, operation_type::Insert, DISTRICT_TABLE_ID,
                     district_rec_ptr);

      ee->insert(st);

      ee->txn_end(true);

      // CUSTOMERS
      for (c_itr = 0; c_itr < customers_per_district; c_itr++) {
        txn_id++;
        ee->txn_begin();

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
            customers_init_payment_cnt, customers_init_delivery_cnt);

        log_str = get_data(customer_rec_ptr, customer_table_schema);
        cout << "customer ::" << log_str << endl;

        st = statement(txn_id, operation_type::Insert, CUSTOMER_TABLE_ID,
                       customer_rec_ptr);

        ee->insert(st);
        ee->txn_end(true);

        // HISTORY

        txn_id++;
        ee->txn_begin();

        int h_w_id = w_itr;
        int h_d_id = d_itr;
        std::string h_data = get_rand_astring(name_len);

        record* history_rec_ptr = new history_record(history_table_schema,
                                                     c_itr, d_itr, w_itr,
                                                     h_w_id, h_d_id, c_ts,
                                                     history_init_amount,
                                                     h_data);

        log_str = get_data(history_rec_ptr, history_table_schema);
        cout << "history ::" << log_str << endl;

        st = statement(txn_id, operation_type::Insert, HISTORY_TABLE_ID,
                       history_rec_ptr);

        ee->insert(st);
        ee->txn_end(true);
      }

      // ORDERS
      for (o_itr = 0; o_itr < customers_per_district; o_itr++) {
        txn_id++;
        ee->txn_begin();

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

        log_str = get_data(orders_rec_ptr, orders_table_schema);
        cout << "orders ::" << log_str << endl;

        st = statement(txn_id, operation_type::Insert, ORDERS_TABLE_ID,
                       orders_rec_ptr);

        ee->insert(st);
        ee->txn_end(true);

        // NEW_ORDER

        if (new_order) {
          txn_id++;
          ee->txn_begin();

          record* new_order_rec_ptr = new new_order_record(
              new_order_table_schema, o_itr, d_itr, w_itr);

          log_str = get_data(new_order_rec_ptr, new_order_table_schema);
          cout << "new_order ::" << log_str << endl;

          st = statement(txn_id, operation_type::Insert, NEW_ORDER_TABLE_ID,
                         new_order_rec_ptr);

          ee->insert(st);
          ee->txn_end(true);
        }

        // ORDER_LINE
        for (ol_itr = 0; ol_itr < o_ol_cnt; ol_itr++) {
          txn_id++;
          ee->txn_begin();

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

          log_str = get_data(order_line_rec_ptr, order_line_table_schema);
          cout << "order_line ::" << log_str << endl;

          st = statement(txn_id, operation_type::Insert, ORDER_LINE_TABLE_ID,
                         order_line_rec_ptr);

          ee->insert(st);
          ee->txn_end(true);
        }

      }

    }

    // STOCK
    for (s_i_itr = 0; s_i_itr < item_count; s_i_itr++) {

      txn_id++;
      ee->txn_begin();

      bool s_original = get_rand_bool(stock_original_ratio);
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

      log_str = get_data(stock_rec_ptr, stock_table_schema);
      cout << "stock ::" << log_str << endl;

      st = statement(txn_id, operation_type::Insert, STOCK_TABLE_ID,
                     stock_rec_ptr);

      ee->insert(st);

      ee->txn_end(true);
    }

  }
}

void tpcc_benchmark::load(engine* ee) {

  cout << "Load items " << endl;
  load_items(ee);

  cout << "Load warehouses " << endl;
  load_warehouses(ee);

}

void tpcc_benchmark::do_delivery(engine* ee) {
  cout << "Delivery " << endl;
  /*
   "getNewOrder": "SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID > -1 LIMIT 1", #
   "deleteNewOrder": "DELETE FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID = ?", # d_id, w_id, no_o_id
   "getCId": "SELECT O_C_ID FROM ORDERS WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?", # no_o_id, d_id, w_id
   "updateOrders": "UPDATE ORDERS SET O_CARRIER_ID = ? WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?", # o_carrier_id, no_o_id, d_id, w_id
   "updateOrderLine": "UPDATE ORDER_LINE SET OL_DELIVERY_D = ? WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?", # o_entry_d, no_o_id, d_id, w_id
   "sumOLAmount": "SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?", # no_o_id, d_id, w_id
   "updateCustomer": "UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + ? WHERE C_ID = ? AND C_D_ID = ? AND C_W_ID = ?", # ol_total, c_id, d_id, w_id
   */

  schema* warehouse_table_schema = conf.db->tables->at(WAREHOUSE_TABLE_ID)->sptr;
  schema* district_table_schema = conf.db->tables->at(DISTRICT_TABLE_ID)->sptr;
  schema* customer_table_schema = conf.db->tables->at(CUSTOMER_TABLE_ID)->sptr;
  schema* history_table_schema = conf.db->tables->at(HISTORY_TABLE_ID)->sptr;
  schema* orders_table_schema = conf.db->tables->at(ORDERS_TABLE_ID)->sptr;
  schema* order_line_table_schema = conf.db->tables->at(ORDER_LINE_TABLE_ID)
      ->sptr;
  schema* new_order_table_schema = conf.db->tables->at(NEW_ORDER_TABLE_ID)->sptr;
  schema* stock_table_schema = conf.db->tables->at(STOCK_TABLE_ID)->sptr;

  record* rec_ptr;
  statement st;
  vector<int> field_ids;
  std::string empty;

  txn_id++;
  ee->txn_begin();

  unsigned int d_itr;

  int w_id = get_rand_int(0, warehouse_count);
  int o_carrier_id = get_rand_int(orders_min_carrier_id, orders_max_carrier_id);
  double ol_delivery_ts = static_cast<double>(time(NULL));

  for (d_itr = 0; d_itr < districts_per_warehouse; d_itr++) {
    cout << "d_itr :: " << d_itr << " w_id :: " << w_id << endl;

    // getNewOrder
    rec_ptr = new new_order_record(new_order_table_schema, 0, d_itr, w_id);

    st = statement(txn_id, operation_type::Select, NEW_ORDER_TABLE_ID, rec_ptr,
                   0, new_order_table_schema);

    std::string new_order_str = ee->select(st);

    if (new_order_str.empty()) {
      ee->txn_end(false);
      return;
    }
    cout << "new_order :: " << new_order_str << endl;

    // deleteNewOrder
    rec_ptr = deserialize_to_record(new_order_str, new_order_table_schema,
                                    false);

    int o_id = std::stoi(rec_ptr->get_data(0));
    cout << "o_id :: " << o_id << endl;

    st = statement(txn_id, operation_type::Delete, NEW_ORDER_TABLE_ID, rec_ptr);

    ee->remove(st);

    // getCId
    rec_ptr = new orders_record(orders_table_schema, o_id, 0, d_itr, w_id, 0, 0,
                                0, 0);

    st = statement(txn_id, operation_type::Select, ORDERS_TABLE_ID, rec_ptr, 0,
                   orders_table_schema);

    std::string orders_str = ee->select(st);

    if (orders_str.empty()) {
      ee->txn_end(false);
      return;
    }
    cout << "orders :: " << orders_str << endl;

    rec_ptr = deserialize_to_record(orders_str, orders_table_schema, false);

    int c_id = std::stoi(rec_ptr->get_data(1));

    cout << "c_id :: " << c_id << endl;

    // updateOrders

    int o_carrier_id = get_rand_int(orders_min_carrier_id,
                                    orders_max_carrier_id);

    rec_ptr->set_int(5, o_carrier_id);
    field_ids = {5};  // carried id

    st = statement(txn_id, operation_type::Update, ORDERS_TABLE_ID, rec_ptr,
                   field_ids);

    ee->update(st);

    // updateOrderLine
    double ol_ts = static_cast<double>(time(NULL));

    rec_ptr = new order_line_record(order_line_table_schema, o_id, d_itr, w_id,
                                    0, 0, 0, ol_ts, 0, 0, empty);

    field_ids = {6};  // ol_ts

    st = statement(txn_id, operation_type::Update, ORDER_LINE_TABLE_ID, rec_ptr,
                   field_ids);

    ee->update(st);

    //sumOLAmount
    rec_ptr = new order_line_record(order_line_table_schema, o_id, d_itr, w_id,
                                    0, 0, 0, 0, 0, 0, empty);

    st = statement(txn_id, operation_type::Select, ORDER_LINE_TABLE_ID, rec_ptr,
                   0, order_line_table_schema);

    std::string order_line_str = ee->select(st);

    if (order_line_str.empty()) {
      ee->txn_end(false);
      return;
    }
    cout << "order_line :: " << order_line_str << endl;

    rec_ptr = deserialize_to_record(order_line_str, order_line_table_schema,
                                    false);

    double ol_amount = std::stod(rec_ptr->get_data(8));
    cout << "ol_amount :: " << ol_amount << endl;

    // updateCustomer

    rec_ptr = new customer_record(customer_table_schema, c_id, d_itr, w_id,
                                  empty, empty, empty, empty, 0, 0, 0, 0, 0, 0,
                                  0);

    st = statement(txn_id, operation_type::Update, CUSTOMER_TABLE_ID, rec_ptr,
                   0, customer_table_schema);

    std::string customer_str = ee->select(st);

    if (customer_str.empty()) {
      ee->txn_end(false);
      return;
    }
    cout << "customer :: " << customer_str << endl;

    rec_ptr = deserialize_to_record(customer_str, customer_table_schema, false);

    double orig_balance = std::stod(rec_ptr->get_data(16));  // balance
    cout << "orig_balance :: " << orig_balance << endl;

    field_ids = {16};  // ol_ts

    rec_ptr->set_double(16, orig_balance + ol_amount);

    st = statement(txn_id, operation_type::Update, CUSTOMER_TABLE_ID, rec_ptr,
                   field_ids);

    ee->update(st);
  }

  ee->txn_end(true);

}

void tpcc_benchmark::do_new_order(engine* ee) {

}

void tpcc_benchmark::do_order_status(engine* ee) {

}

void tpcc_benchmark::do_payment(engine* ee) {

}

void tpcc_benchmark::do_stock_level(engine* ee) {

}

void tpcc_benchmark::execute(engine* ee) {

  unsigned int txn_itr;
  status ss(conf.num_txns);

  for (txn_itr = 0; txn_itr < conf.num_txns; txn_itr++) {
    double u = uniform_dist[txn_itr];

    do_delivery(ee);

    /*
     if (u <= 0.04) {
     // STOCK_LEVEL
     do_stock_level(ee);
     } else if (u <= 0.08) {
     // DELIVERY
     do_delivery(ee);
     } else if (u <= 0.12) {
     // ORDER_STATUS
     do_order_status(ee);
     } else if (u <= 0.55) {
     // PAYMENT
     do_payment(ee);
     } else {
     // NEW_ORDER
     do_new_order(ee);
     }
     */

    //ss.display();
  }

  //display_stats(ee, tm.duration(), conf.num_txns);
}

