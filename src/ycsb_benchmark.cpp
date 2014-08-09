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

    hash_id = key;
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
  schema* user_table_schema = new schema(cols);
  pmemalloc_activate(user_table_schema);

  table* user_table = new table("user", user_table_schema, 1, conf);
  pmemalloc_activate(user_table);

  // PRIMARY INDEX
  for (int itr = 1; itr <= conf.ycsb_num_val_fields; itr++) {
    cols[itr].enabled = 0;
  }

  schema* user_table_index_schema = new schema(cols);
  pmemalloc_activate(user_table_index_schema);

  table_index* key_index = new table_index(user_table_index_schema,
                                           conf.ycsb_num_val_fields + 1, conf);
  pmemalloc_activate(key_index);
  user_table->indices->push_back(key_index);

  return user_table;
}

ycsb_benchmark::ycsb_benchmark(config& _conf)
    : benchmark(_conf),
      conf(_conf),
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

  user_table_schema = conf.db->tables->at(USER_TABLE_ID)->sptr;

  if (conf.recovery) {
    conf.num_keys = 1000;
    conf.ycsb_per_writes = 0.5;
  }

  if (conf.ycsb_update_one == false) {
    for (int itr = 1; itr <= conf.ycsb_num_val_fields; itr++)
      update_field_ids.push_back(itr);
  } else {
    // Update only first field
    update_field_ids.push_back(1);
  }

  // Generate skewed dist
  simple_skew(simple_dist, conf.ycsb_skew, conf.num_keys,
              conf.num_txns * conf.ycsb_tuples_per_txn);
  uniform(uniform_dist, conf.num_txns);
}

void ycsb_benchmark::load(engine* ee) {

  unsigned int usertable_id = 0;
  unsigned int usertable_index_id = 0;
  schema* usertable_schema = conf.db->tables->at(usertable_id)->sptr;
  unsigned int txn_itr;
  status ss(conf.num_keys);

  ee->txn_begin();

  for (txn_itr = 0; txn_itr < conf.num_keys; txn_itr++) {

    if (txn_itr % conf.load_batch_size == 0) {
      ee->txn_end(true);
      txn_id++;
      ee->txn_begin();
    }

    // LOAD
    int key = txn_itr;
    std::string value = get_rand_astring(conf.ycsb_field_size);

    record* rec_ptr = new usertable_record(usertable_schema, key, value,
                                           conf.ycsb_num_val_fields, false);

    statement st(txn_id, operation_type::Insert, usertable_id, rec_ptr);

    ee->load(st);

    ss.display();
  }

  ee->txn_end(true);
}

void ycsb_benchmark::do_update(engine* ee, unsigned int tid) {

// UPDATE
  std::string updated_val(conf.ycsb_field_size, 'x');
  int zipf_dist_offset = txn_id * conf.ycsb_tuples_per_txn;
  txn_id++;
  int rc;

  TIMER(ee->txn_begin())

  for (int stmt_itr = 0; stmt_itr < conf.ycsb_tuples_per_txn; stmt_itr++) {

    int key = simple_dist[zipf_dist_offset + stmt_itr];

    record* rec_ptr = new usertable_record(user_table_schema, key, updated_val,
                                           conf.ycsb_num_val_fields,
                                           conf.ycsb_update_one);

    statement st(txn_id, operation_type::Update, USER_TABLE_ID, rec_ptr,
                 update_field_ids);

    TIMER(rc = ee->update(st))
    if (rc != 0) {
      TIMER(ee->txn_end(false))
      return;
    }
  }

  TIMER(ee->txn_end(true))
}

void ycsb_benchmark::do_read(engine* ee, unsigned int tid) {

// SELECT
  int zipf_dist_offset = txn_id * conf.ycsb_tuples_per_txn;
  txn_id++;
  std::string empty;
  std::string rc;

  TIMER(ee->txn_begin())

  for (int stmt_itr = 0; stmt_itr < conf.ycsb_tuples_per_txn; stmt_itr++) {

    int key = simple_dist[zipf_dist_offset + stmt_itr];

    record* rec_ptr = new usertable_record(user_table_schema, key, empty,
                                           conf.ycsb_num_val_fields, false);

    statement st(txn_id, operation_type::Select, USER_TABLE_ID, rec_ptr, 0,
                 user_table_schema);

    TIMER(ee->select(st))
  }

  TIMER(ee->txn_end(true))
}

void ycsb_benchmark::sim_crash(engine* ee) {

  // UPDATE
  vector<int> field_ids;
  for (int itr = 1; itr <= conf.ycsb_num_val_fields; itr++)
    field_ids.push_back(itr);

  std::string updated_val(conf.ycsb_field_size, 'x');
  int zipf_dist_offset = 0;

  ee->txn_begin();

  for (int stmt_itr = 0; stmt_itr < conf.ycsb_tuples_per_txn; stmt_itr++) {

    int key = simple_dist[zipf_dist_offset + stmt_itr];

    record* rec_ptr = new usertable_record(user_table_schema, key, updated_val,
                                           conf.ycsb_num_val_fields,
                                           conf.ycsb_update_one);

    statement st(txn_id, operation_type::Update, USER_TABLE_ID, rec_ptr,
                 field_ids);

    ee->update(st);

  }

  // Don't finish the transaction
  //ee->txn_end(true);
}

void ycsb_benchmark::handler(engine* ee, unsigned int tid) {
  unsigned int num_thds = conf.num_executors;
  unsigned int per_thd_txns = conf.num_txns / num_thds;
  unsigned int start_txn_itr = per_thd_txns * tid;
  unsigned int end_txn_itr = start_txn_itr + per_thd_txns;
  unsigned int txn_itr;
  status ss(per_thd_txns);

  //cout<<"per thd txns :: "<<per_thd_txns<<endl;
  //cout<<"tid :: "<<tid<<" engine :: "<<ee->de<< endl;

  for (txn_itr = start_txn_itr; txn_itr < end_txn_itr; txn_itr++) {
    double u = uniform_dist[txn_itr];

    if (u < conf.ycsb_per_writes) {
      do_update(ee, tid);
    } else {
      do_read(ee, tid);
    }

    if (tid == 0)
      ss.display();
  }
}
