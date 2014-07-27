// TPCC BENCHMARK

#include "tpcc_benchmark.h"

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

    conf.sp->init = 1;
  } else {
    //cout << "Recovery Mode " << endl;
    database* db = (database*) conf.sp->ptrs[0];
    db->reset(conf);
    conf.db = db;
  }
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
               int s_d_0_id, int s_d_1_id, int s_d_2_id, int s_d_3_id,
               int s_d_4_id, int s_d_5_id, int s_d_6_id, int s_d_7_id,
               int s_d_8_id, int s_d_9_id, int s_ytd, int s_order_cnt,
               int s_remote_cnt, const std::string& s_data)
      : record(sptr) {

    char* vc = NULL;
    std::sprintf(&(data[sptr->columns[0].offset]), "%d", s_i_id);
    std::sprintf(&(data[sptr->columns[1].offset]), "%d", s_w_id);
    std::sprintf(&(data[sptr->columns[2].offset]), "%d", s_quantity);
    std::sprintf(&(data[sptr->columns[3].offset]), "%d", s_d_0_id);
    std::sprintf(&(data[sptr->columns[4].offset]), "%d", s_d_1_id);
    std::sprintf(&(data[sptr->columns[5].offset]), "%d", s_d_2_id);
    std::sprintf(&(data[sptr->columns[6].offset]), "%d", s_d_3_id);
    std::sprintf(&(data[sptr->columns[7].offset]), "%d", s_d_4_id);
    std::sprintf(&(data[sptr->columns[8].offset]), "%d", s_d_5_id);
    std::sprintf(&(data[sptr->columns[9].offset]), "%d", s_d_6_id);
    std::sprintf(&(data[sptr->columns[10].offset]), "%d", s_d_7_id);
    std::sprintf(&(data[sptr->columns[11].offset]), "%d", s_d_8_id);
    std::sprintf(&(data[sptr->columns[12].offset]), "%d", s_d_9_id);

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
  for (int itr = 3; itr <= cols.size(); itr++) {
    cols[itr].enabled = 0;
  }

  schema* p_index_schema = new schema(cols);
  pmemalloc_activate(p_index_schema);

  table_index* p_index = new table_index(p_index_schema, cols.size(), conf);
  pmemalloc_activate(p_index);
  orders->indices->push_back(p_index);

  // SECONDARY INDEX
  cols[0].enabled = 0;
  cols[3].enabled = 1;

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

  for (int f_itr = 0; f_itr <= 3; f_itr++) {
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

// LOADING
double get_rand_double(double d_min, double d_max) {
  double f = (double) rand() / RAND_MAX;
  return d_min + f * (d_max - d_min);
}

void tpcc_benchmark::load_warehouse(engine* ee) {
  int num_warehouses = conf.tpcc_num_warehouses;

  unsigned int warehouse_index_id = 0;
  schema* warehouse_table_schema = conf.db->tables->at(WAREHOUSE_TABLE_ID)->sptr;
  unsigned int w_itr;

  double tax = 0.1f;
  double ytd = 300000.00f;

  for (w_itr = 0; w_itr < num_warehouses; w_itr++) {
    txn_id++;
    ee->txn_begin();

    // INSERT
    std::string name = random_string(3);
    std::string zip = random_string(5);
    std::string state = random_string(2);

    record* rec_ptr = new warehouse_record(warehouse_table_schema, w_itr, name,
                                           zip, state, tax, ytd);

    std::string key_str = get_data(rec_ptr, warehouse_table_schema);
    cout << "warehouse ::" << key_str << endl;

    statement st(txn_id, operation_type::Insert, WAREHOUSE_TABLE_ID, rec_ptr);

    ee->insert(st);

    ee->txn_end(true);
  }
}

void tpcc_benchmark::load_district(engine* ee) {
  int num_warehouses = conf.tpcc_num_warehouses;
  int num_districts_per_warehouse = 2;

  unsigned int district_index_id = 0;
  schema* district_table_schema = conf.db->tables->at(DISTRICT_TABLE_ID)->sptr;
  unsigned int w_itr, d_itr;

  double tax = 0.1f;
  double ytd = 30000.00f;  // different from warehouse
  int next_d_o_id = 3000;

  for (w_itr = 0; w_itr < num_warehouses; w_itr++) {
    for (d_itr = 0; d_itr < num_districts_per_warehouse; d_itr++) {

      txn_id++;
      ee->txn_begin();

      int dist = w_itr * num_districts_per_warehouse + d_itr;
      std::string name = random_string(3);
      std::string zip = random_string(5);
      std::string state = random_string(2);

      record* rec_ptr = new district_record(district_table_schema, w_itr, dist,
                                            name, zip, state, tax, ytd,
                                            next_d_o_id);

      std::string key_str = get_data(rec_ptr, district_table_schema);
      cout << "district ::" << key_str << endl;

      statement st(txn_id, operation_type::Insert, DISTRICT_TABLE_ID, rec_ptr);

      ee->insert(st);

      ee->txn_end(true);
    }
  }
}

void tpcc_benchmark::load_item(engine* ee) {
  int num_items = 3;

  unsigned int item_index_id = 0;
  schema* item_table_schema = conf.db->tables->at(ITEM_TABLE_ID)->sptr;
  unsigned int i_itr;

  double tax = 0.1f;
  double ytd = 30000.00f;  // different from warehouse
  int next_d_o_id = 3000;

  for (i_itr = 0; i_itr < num_items; i_itr++) {

    txn_id++;
    ee->txn_begin();

    int i_im_id = i_itr * 10;
    std::string name = random_string(3);
    double price = get_rand_double(1.0, 100.0);

    record* rec_ptr = new item_record(item_table_schema, i_itr, i_im_id, name,
                                      price);

    std::string key_str = get_data(rec_ptr, item_table_schema);
    cout << "item ::" << key_str << endl;

    statement st(txn_id, operation_type::Insert, ITEM_TABLE_ID, rec_ptr);

    ee->insert(st);

    ee->txn_end(true);
  }

}

void tpcc_benchmark::load(engine* ee) {

  cout << "Load warehouse " << endl;
  load_warehouse(ee);

  cout << "Load district " << endl;
  load_district(ee);

  cout << "Load item " << endl;
  load_item(ee);

}

void tpcc_benchmark::execute(engine* ee) {
}
