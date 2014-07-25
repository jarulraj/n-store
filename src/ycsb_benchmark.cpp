// YCSB BENCHMARK

#include "ycsb_benchmark.h"
#include "field.h"
#include "record.h"
#include "statement.h"
#include "utils.h"
#include "nstore.h"

#include "libpm.h"
#include "plist.h"

using namespace std;

class usertable_record : public record {
 public:
  usertable_record(schema* _sptr, int _key, const std::string& _val)
      : record(_sptr),
        vc(NULL) {
    if (!_val.empty()) {
      size_t _val_len = _val.size();
      vc = new char[_val_len + 1];
      strcpy(vc, _val.c_str());
    }

    std::sprintf(&(data[sptr->columns[0].offset]), "%d", _key);
    std::sprintf(&(data[sptr->columns[1].offset]), "%p", vc);
  }

  char* vc;
};

// USERTABLE
table* create_usertable(config& conf) {

  vector<field_info> cols;
  off_t offset;

  offset = 0;
  field_info col1(offset, 10, 10, field_type::INTEGER, 1, 1);
  offset += col1.ser_len;
  field_info col2(offset, sizeof(void*), conf.sz_value, field_type::VARCHAR, 0,
                  1);
  offset += col2.ser_len;

  cols.push_back(col1);
  cols.push_back(col2);

  // SCHEMA
  schema* usertable_schema = new schema(cols);
  pmemalloc_activate(usertable_schema);

  table* usertable = new table("usertable", usertable_schema, 1, conf);
  pmemalloc_activate(usertable);

  // PRIMARY INDEX
  cols[1].enabled = 0;
  schema* usertable_index_schema = new schema(cols);
  pmemalloc_activate(usertable_index_schema);

  table_index* key_index = new table_index(usertable_index_schema, 2, conf);
  pmemalloc_activate(key_index);
  usertable->indices->push_back(key_index);

  return usertable;
}

ycsb_benchmark::ycsb_benchmark(config& _conf)
    : conf(_conf),
      txn_id(0) {

  // Initialization mode
  if (conf.sp->init == 0) {
    //cout << "Initialization Mode" << endl;

    database* db = new database(conf);
    conf.sp->ptrs[0] = db;
    pmemalloc_activate(db);
    conf.db = db;

    table* usertable = create_usertable(conf);
    db->tables->push_back(usertable);

    conf.sp->init = 1;
  } else {
    //cout << "Recovery Mode " << endl;
    database* db = (database*) conf.sp->ptrs[0];
    db->reset(conf);
    conf.db = db;

  }

  // Generate Zipf dist
  zipf(zipf_dist, conf.skew, conf.num_keys, conf.num_txns);
  uniform(uniform_dist, conf.num_txns);
}

workload & ycsb_benchmark::get_dataset() {

  load.txns.clear();

  unsigned int usertable_id = 0;
  unsigned int usertable_index_id = 0;
  schema* usertable_schema = conf.db->tables->at(usertable_id)->sptr;
  unsigned int txn_itr;

  for (txn_itr = 0; txn_itr < conf.num_keys; txn_itr++, txn_id++) {

    // INSERT
    int key = txn_itr;
    std::string value = random_string(conf.sz_value);

    record* rec_ptr = new usertable_record(usertable_schema, key, value);

    statement st(txn_id, operation_type::Insert, usertable_id, rec_ptr);

    vector<statement> stmts = { st };
    transaction txn(txn_itr, stmts);
    load.txns.push_back(txn);
  }

  //cout << load.txns.size() << " dataset transactions " << endl;

  return load;
}

workload & ycsb_benchmark::get_workload() {

  load.txns.clear();

  if (conf.per_writes == 0)
    load.read_only = true;

  unsigned int usertable_id = 0;
  unsigned int usertable_index_id = 0;
  unsigned int txn_itr;
  schema* usertable_schema = conf.db->tables->at(usertable_id)->sptr;
  std::string empty;

  for (txn_itr = 0; txn_itr < conf.num_txns; txn_itr++, txn_id++) {

    int key = zipf_dist[txn_itr];
    double u = uniform_dist[txn_itr];

    if (u < conf.per_writes) {
      // UPDATE
      std::string updated_val(conf.sz_value, 'x');

      record* rec_ptr = new usertable_record(usertable_schema, key,
                                             updated_val);

      vector<int> field_ids = { 1 };
      statement st(txn_id, operation_type::Update, usertable_id, rec_ptr,
                   field_ids);

      vector<statement> stmts = { st };

      transaction txn(txn_itr, stmts);
      load.txns.push_back(txn);

    } else {

      // SELECT
      record* rec_ptr = new usertable_record(usertable_schema, key, empty);

      std::string key_str = std::to_string(key) + " ";

      statement st(txn_id, operation_type::Select, usertable_id, rec_ptr,
                   usertable_index_id, usertable_schema, key_str);

      vector<statement> stmts = { st };

      transaction txn(txn_itr, stmts);
      load.txns.push_back(txn);
    }
  }

  //cout << load.txns.size() << " workload transactions " << endl;

  return load;
}
