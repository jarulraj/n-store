// YCSB BENCHMARK

#include "ycsb_benchmark.h"
#include "field.h"
#include "record.h"
#include "statement.h"
#include "utils.h"
#include "nstore.h"
#include "status.h"
#include "libpm.h"
#include "plist.h"

using namespace std;

class usertable_record : public record {
 public:
  usertable_record(schema* _sptr, int key, const std::string& val,
                   int num_val_fields, bool update_one)
      : record(_sptr) {

    set_int(0, key);

    if (val.empty())
      return;

    if (!update_one) {
      for (int itr = 1; itr <= num_val_fields; itr++) {
        set_varchar(itr, val);
      }
    } else
      set_varchar(1, val);
  }

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

  btype = benchmark_type::YCSB;

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

void ycsb_benchmark::load(engine* ee) {

  unsigned int usertable_id = 0;
  unsigned int usertable_index_id = 0;
  schema* usertable_schema = conf.db->tables->at(usertable_id)->sptr;
  unsigned int txn_itr;
  status ss(conf.num_keys);

  for (txn_itr = 0; txn_itr < conf.num_keys; txn_itr++, txn_id++) {

    ee->txn_begin();

    // INSERT
    int key = txn_itr;
    std::string value = get_rand_astring(conf.ycsb_field_size);

    record* rec_ptr = new usertable_record(usertable_schema, key, value,
                                           conf.ycsb_num_val_fields, false);

    statement st(txn_id, operation_type::Insert, usertable_id, rec_ptr);

    ee->insert(st);

    ee->txn_end(true);

    ss.display();
  }

}

void ycsb_benchmark::do_update(engine* ee, unsigned int txn_itr,
                               schema* usertable_schema,
                               const vector<int>& field_ids) {

// UPDATE
  std::string updated_val(conf.ycsb_field_size, 'x');
  int zipf_dist_offset = txn_itr * conf.ycsb_tuples_per_txn;
  unsigned int usertable_id = 0;
  int rc;

  TIMER(ee->txn_begin())

  for (int stmt_itr = 0; stmt_itr < conf.ycsb_tuples_per_txn; stmt_itr++) {

    int key = zipf_dist[zipf_dist_offset + stmt_itr];

    record* rec_ptr = new usertable_record(usertable_schema, key, updated_val,
                                           conf.ycsb_num_val_fields,
                                           conf.ycsb_update_one);

    statement st(txn_id, operation_type::Update, usertable_id, rec_ptr,
                 field_ids);

    TIMER(ee->update(st))

    if (rc < 0)
      perror("update");
  }

  TIMER(ee->txn_end(true))
}

void ycsb_benchmark::do_read(engine* ee, unsigned int txn_itr,
                             schema* usertable_schema) {

// SELECT
  int zipf_dist_offset = txn_itr * conf.ycsb_tuples_per_txn;
  unsigned int usertable_id = 0;
  unsigned int usertable_index_id = 0;
  std::string empty;
  int rc;

  TIMER(ee->txn_begin())

  for (int stmt_itr = 0; stmt_itr < conf.ycsb_tuples_per_txn; stmt_itr++) {

    int key = zipf_dist[zipf_dist_offset + stmt_itr];

    record* rec_ptr = new usertable_record(usertable_schema, key, empty,
                                           conf.ycsb_num_val_fields, false);

    statement st(txn_id, operation_type::Select, usertable_id, rec_ptr,
                 usertable_index_id, usertable_schema);

    TIMER(ee->select(st))
  }

  TIMER(ee->txn_end(true))
}

void ycsb_benchmark::execute_one(engine* ee) {

  // UPDATE
  unsigned int usertable_id = 0;
  schema* usertable_schema = conf.db->tables->at(usertable_id)->sptr;

  vector<int> field_ids;
  for (int itr = 1; itr <= conf.ycsb_num_val_fields; itr++)
    field_ids.push_back(itr);

  std::string updated_val(conf.ycsb_field_size, 'x');
  int zipf_dist_offset = 0;

  ee->txn_begin();

  for (int stmt_itr = 0; stmt_itr < conf.ycsb_tuples_per_txn; stmt_itr++) {

    int key = zipf_dist[zipf_dist_offset + stmt_itr];

    record* rec_ptr = new usertable_record(usertable_schema, key, updated_val,
                                           conf.ycsb_num_val_fields,
                                           conf.ycsb_update_one);

    statement st(txn_id, operation_type::Update, usertable_id, rec_ptr,
                 field_ids);

    ee->update(st);

  }

  // Don't finish the transaction
  //ee->txn_end(true);
}

void ycsb_benchmark::execute(engine* ee) {

  unsigned int usertable_id = 0;
  unsigned int txn_itr;
  schema* usertable_schema = conf.db->tables->at(usertable_id)->sptr;
  status ss(conf.num_txns);

  vector<int> field_ids;
  if (conf.ycsb_update_one == false) {
    for (int itr = 1; itr <= conf.ycsb_num_val_fields; itr++)
      field_ids.push_back(itr);
  } else {
    // Update only first field
    field_ids.push_back(1);
  }

  for (txn_itr = 0; txn_itr < conf.num_txns; txn_itr++, txn_id++) {
    double u = uniform_dist[txn_itr];

    if (u < conf.ycsb_per_writes) {
      do_update(ee, txn_itr, usertable_schema, field_ids);
    } else {
      do_read(ee, txn_itr, usertable_schema);
    }

    ss.display();
  }

  display_stats(ee, tm.duration(), conf.num_txns);
}
