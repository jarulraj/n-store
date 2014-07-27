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

    table* warehouse_table = create_warehouse();
    db->tables->push_back(warehouse_table);

    table* district_table = create_district();
    db->tables->push_back(district_table);

    table* item_table = create_item();
    db->tables->push_back(item_table);

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
  field = field_info(offset, 12, 16, field_type::VARCHAR, 0, 1);
  offset += field.ser_len;
  cols.push_back(field);
  field = field_info(offset, 12, 32, field_type::VARCHAR, 0, 1);
  offset += field.ser_len;
  cols.push_back(field);
  field = field_info(offset, 12, 32, field_type::VARCHAR, 0, 1);
  offset += field.ser_len;
  cols.push_back(field);
  field = field_info(offset, 12, 32, field_type::VARCHAR, 0, 1);
  offset += field.ser_len;
  cols.push_back(field);
  field = field_info(offset, 12, 2, field_type::VARCHAR, 0, 1);
  offset += field.ser_len;
  cols.push_back(field);
  field = field_info(offset, 12, 9, field_type::VARCHAR, 0, 1);
  offset += field.ser_len;
  cols.push_back(field);
  field = field_info(offset, 15, 15, field_type::DOUBLE, 1, 1);
  offset += field.ser_len;
  cols.push_back(field);
  field = field_info(offset, 15, 15, field_type::DOUBLE, 1, 1);
  offset += field.ser_len;
  cols.push_back(field);

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

void tpcc_benchmark::load_warehouse(engine* ee) {
  int num_warehouses = conf.tpcc_num_warehouses;

  unsigned int warehouse_table_id = 0;
  unsigned int warehouse_index_id = 0;
  schema* warehouse_table_schema = conf.db->tables->at(warehouse_table_id)->sptr;
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

    statement st(txn_id, operation_type::Insert, warehouse_table_id, rec_ptr);

    ee->insert(st);

    ee->txn_end(true);
  }
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
  field = field_info(offset, 12, 16, field_type::VARCHAR, 0, 1);
  offset += field.ser_len;
  cols.push_back(field);
  field = field_info(offset, 12, 32, field_type::VARCHAR, 0, 1);
  offset += field.ser_len;
  cols.push_back(field);
  field = field_info(offset, 12, 32, field_type::VARCHAR, 0, 1);
  offset += field.ser_len;
  cols.push_back(field);
  field = field_info(offset, 12, 32, field_type::VARCHAR, 0, 1);
  offset += field.ser_len;
  cols.push_back(field);
  field = field_info(offset, 12, 2, field_type::VARCHAR, 0, 1);
  offset += field.ser_len;
  cols.push_back(field);
  field = field_info(offset, 12, 9, field_type::VARCHAR, 0, 1);
  offset += field.ser_len;
  cols.push_back(field);
  field = field_info(offset, 15, 15, field_type::DOUBLE, 1, 1);
  offset += field.ser_len;
  cols.push_back(field);
  field = field_info(offset, 15, 15, field_type::DOUBLE, 1, 1);
  offset += field.ser_len;
  cols.push_back(field);
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

void tpcc_benchmark::load_district(engine* ee) {
  int num_warehouses = conf.tpcc_num_warehouses;
  int num_districts_per_warehouse = 2;

  unsigned int district_table_id = 1;
  unsigned int district_index_id = 0;
  schema* district_table_schema = conf.db->tables->at(district_table_id)->sptr;
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

      statement st(txn_id, operation_type::Insert, district_table_id, rec_ptr);

      ee->insert(st);

      ee->txn_end(true);
    }
  }
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

double get_rand_double(double d_min, double d_max) {
  double f = (double) rand() / RAND_MAX;
  return d_min + f * (d_max - d_min);
}

void tpcc_benchmark::load_item(engine* ee) {
  int num_items = 3;

  unsigned int item_table_id = 2;
  unsigned int item_index_id = 0;
  schema* item_table_schema = conf.db->tables->at(item_table_id)->sptr;
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

    statement st(txn_id, operation_type::Insert, item_table_id, rec_ptr);

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
