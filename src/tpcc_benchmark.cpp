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

    conf.sp->init = 1;
  } else {
    //cout << "Recovery Mode " << endl;
    database* db = (database*) conf.sp->ptrs[0];
    db->reset(conf);
    conf.db = db;
  }
}

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

// WAREHOUSE
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
  field_info w_id(offset, 10, 10, field_type::INTEGER, 1, 1);
  offset += w_id.ser_len;
  cols.push_back(w_id);
  field_info val = field_info(offset, 12, 16, field_type::VARCHAR, 0, 1);
  offset += val.ser_len;
  cols.push_back(val);
  val = field_info(offset, 12, 32, field_type::VARCHAR, 0, 1);
  offset += val.ser_len;
  cols.push_back(val);
  val = field_info(offset, 12, 32, field_type::VARCHAR, 0, 1);
  offset += val.ser_len;
  cols.push_back(val);
  val = field_info(offset, 12, 32, field_type::VARCHAR, 0, 1);
  offset += val.ser_len;
  cols.push_back(val);
  val = field_info(offset, 12, 2, field_type::VARCHAR, 0, 1);
  offset += val.ser_len;
  cols.push_back(val);
  val = field_info(offset, 12, 9, field_type::VARCHAR, 0, 1);
  offset += val.ser_len;
  cols.push_back(val);
  val = field_info(offset, 15, 15, field_type::DOUBLE, 1, 1);
  offset += val.ser_len;
  cols.push_back(val);
  val = field_info(offset, 15, 15, field_type::DOUBLE, 1, 1);
  offset += val.ser_len;
  cols.push_back(val);

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
  int num_warehouses = 10;

  unsigned int warehouse_table_id = 0;
  unsigned int warehouse_index_id = 0;
  schema* warehouse_table_schema = conf.db->tables->at(warehouse_table_id)->sptr;
  unsigned int w_itr;

  double tax = 0.1f;
  double ytd = 300000.00f;

  for (w_itr = 0; w_itr < num_warehouses; w_itr++, txn_id++) {

    ee->txn_begin();

    // INSERT
    std::string name = random_string(3);
    std::string zip = random_string(5);
    std::string state = random_string(2);

    record* rec_ptr = new warehouse_record(warehouse_table_schema, w_itr, name,
                                           zip, state, tax, ytd);

    std::string key_str = get_data(rec_ptr, warehouse_table_schema);

    statement st(txn_id, operation_type::Insert, warehouse_table_id, rec_ptr);

    ee->insert(st);

    ee->txn_end(true);
  }

}

void tpcc_benchmark::load(engine* ee) {

  load_warehouse(ee);

}

void tpcc_benchmark::execute(engine* ee) {
}
