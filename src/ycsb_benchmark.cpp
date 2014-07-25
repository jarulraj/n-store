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
  usertable_record(schema* _sptr, int _key, const std::string& _val,
                   int num_val_fields, bool update_one)
      : record(_sptr),
        vc(NULL) {

    std::sprintf(&(data[sptr->columns[0].offset]), "%d", _key);

    if (!_val.empty()) {
      size_t _val_len = _val.size();

      if (!update_one) {
        for (int itr = 1; itr <= num_val_fields; itr++) {
          vc = new char[_val_len + 1];
          strcpy(vc, _val.c_str());
          std::sprintf(&(data[sptr->columns[itr].offset]), "%p", vc);
        }
      } else {
        vc = new char[_val_len + 1];
        strcpy(vc, _val.c_str());
        std::sprintf(&(data[sptr->columns[1].offset]), "%p", vc);
      }
    }

  }

  char* vc;
};

// USERTABLE
table* create_usertable(config& conf) {

  vector<field_info> cols;
  off_t offset;

  offset = 0;
  field_info key(offset, 10, 10, field_type::INTEGER, 1, 1);
  offset += key.ser_len;
  cols.push_back(key);

  for (int itr = 1; itr <= conf.ycsb_num_val_fields; itr++) {
    field_info val = field_info(offset, 12, conf.ycsb_field_size,
                                field_type::VARCHAR, 0, 1);
    offset += val.ser_len;
    cols.push_back(val);
  }

  // SCHEMA
  schema* usertable_schema = new schema(cols);
  pmemalloc_activate(usertable_schema);

  table* usertable = new table("usertable", usertable_schema, 1, conf);
  pmemalloc_activate(usertable);

  // PRIMARY INDEX
  for (int itr = 1; itr <= conf.ycsb_num_val_fields; itr++) {
    cols[itr].enabled = 0;
  }

  schema* usertable_index_schema = new schema(cols);
  pmemalloc_activate(usertable_index_schema);

  table_index* key_index = new table_index(usertable_index_schema,
                                           conf.ycsb_num_val_fields + 1, conf);
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
  zipf(zipf_dist, conf.ycsb_skew, conf.num_keys,
       conf.num_txns * conf.ycsb_tuples_per_txn);
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
    std::string value = random_string(conf.ycsb_field_size);

    record* rec_ptr = new usertable_record(usertable_schema, key, value,
                                           conf.ycsb_num_val_fields, false);

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

  if (conf.ycsb_per_writes == 0)
    load.read_only = true;

  unsigned int usertable_id = 0;
  unsigned int usertable_index_id = 0;
  unsigned int txn_itr;
  schema* usertable_schema = conf.db->tables->at(usertable_id)->sptr;
  std::string empty;

  vector<int> field_ids;
  if (conf.ycsb_update_one == false) {
    for (int itr = 1; itr <= conf.ycsb_num_val_fields; itr++)
      field_ids.push_back(itr);
  } else {
    // Update only first field
    field_ids.push_back(1);
  }

  for (txn_itr = 0; txn_itr < conf.num_txns; txn_itr++, txn_id++) {

    int zipf_dist_offset = txn_itr * conf.ycsb_tuples_per_txn;
    double u = uniform_dist[txn_itr];

    if (u < conf.ycsb_per_writes) {
      // UPDATE
      std::string updated_val(conf.ycsb_field_size, 'x');
      vector<statement> stmts;

      for (int stmt_itr = 0; stmt_itr < conf.ycsb_tuples_per_txn; stmt_itr++) {

        int key = zipf_dist[zipf_dist_offset + stmt_itr];

        record* rec_ptr = new usertable_record(usertable_schema, key,
                                               updated_val,
                                               conf.ycsb_num_val_fields,
                                               conf.ycsb_update_one);

        statement st(txn_id, operation_type::Update, usertable_id, rec_ptr,
                     field_ids);

        stmts.push_back(st);
      }

      transaction txn(txn_itr, stmts);
      load.txns.push_back(txn);

    } else {

      // SELECT
      vector<statement> stmts;

      for (int stmt_itr = 0; stmt_itr < conf.ycsb_tuples_per_txn; stmt_itr++) {

        int key = zipf_dist[zipf_dist_offset + stmt_itr];

        record* rec_ptr = new usertable_record(usertable_schema, key, empty,
                                               conf.ycsb_num_val_fields, false);

        std::string key_str = std::to_string(key) + " ";

        statement st(txn_id, operation_type::Select, usertable_id, rec_ptr,
                     usertable_index_id, usertable_schema, key_str);

        stmts.push_back(st);
      }

      transaction txn(txn_itr, stmts);
      load.txns.push_back(txn);
    }
  }

  //cout << load.txns.size() << " workload transactions " << endl;

  return load;
}
