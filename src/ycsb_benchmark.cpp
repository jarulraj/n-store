// YCSB BENCHMARK

#include "ycsb_benchmark.h"
#include "field.h"
#include "record.h"
#include "statement.h"
#include "utils.h"
#include "nstore.h"

using namespace std;

#define DELIM ' '

class usertable_record : public record {
 public:
  usertable_record(unsigned int _key, char* _val) {

    integer_field* key = new integer_field(_key);
    varchar_field* val = new varchar_field(_val);

    data.push_back(key);
    data.push_back(val);
  }

};

ycsb_benchmark::ycsb_benchmark(config& _conf)
    : conf(_conf),
      s_id(0) {

  // Table 0 : usertable with index on key
  table* usertable = new table("usertable");

  vector<bool> key = { 1, 0 };
  table_index* key_index = new table_index(key);

  usertable->indices.push_back(key_index);
  load.tables.push_back(usertable);

  // Generate Zipf dist
  long part_range = conf.num_keys / conf.num_parts;
  long part_txns = conf.num_txns / conf.num_parts;

  zipf(zipf_dist, conf.skew, part_range, part_txns);
  uniform(uniform_dist, part_txns);

}

workload* ycsb_benchmark::get_dataset() {

  table* usertable_ptr = load.tables.at(0);
  load.txns.clear();

  int part_range = conf.num_keys / conf.num_parts;
  int part_itr, txn_itr, txn_id;

  vector<bool> projection = { 0, 0 };

  for (int txn_itr = 0; txn_itr < part_range; txn_itr++) {

    for (int part_itr = 0; part_itr < conf.num_parts; part_itr++) {
      txn_id = part_range * part_itr + txn_itr;

      int key = txn_id;

      char* value = new char[conf.sz_value];
      random_string(value, conf.sz_value);

      record* rec_ptr = new usertable_record(key, value);

      statement* st = new statement(++s_id, partition_type::Single, part_itr,
                                    operation_type::Insert, usertable_ptr,
                                    rec_ptr, -1, NULL, NULL, projection);

      vector<statement*> stmts = { st };
      transaction txn(stmts);
      load.txns.push_back(txn);
    }

  }

  //cout << load.txns.size() << " dataset transactions " << endl;

  return &load;
}

workload* ycsb_benchmark::get_workload() {

  table* usertable_ptr = load.tables.at(0);
  load.txns.clear();

  int part_range = conf.num_keys / conf.num_parts;
  int part_txns = conf.num_txns / conf.num_parts;
  int part_itr, txn_itr, txn_id;

  vector<bool> projection = { 1, 1 };

  for (txn_itr = 0; txn_itr < part_txns; txn_itr++) {

    for (part_itr = 0; part_itr < conf.num_parts; part_itr++) {
      txn_id = part_range * part_itr + txn_itr;

      int key = conf.zipf_dist[txn_id];
      double u = conf.uniform_dist[txn_id];

      // UPDATE
      if (u < conf.per_writes) {
        char* updated_val = new char[conf.sz_value];
        memset(updated_val, 'x', conf.sz_value);
        updated_val[conf.sz_value - 1] = '\0';

        varchar_field* val = new varchar_field(updated_val);

        record* rec_ptr = new usertable_record(key, NULL);

        statement* st = new statement(++s_id, partition_type::Single, part_itr,
                                      operation_type::Update, usertable_ptr,
                                      rec_ptr, 1, val, NULL, projection);

        vector<statement*> stmts = { st };

        transaction txn(stmts);
        load.txns.push_back(txn);
      } else {

        // SELECT

        record* rec_ptr = new usertable_record(key, NULL);

        table_index* tindx_ptr = usertable_ptr->indices.at(0);

        statement* st = new statement(++s_id, partition_type::Single, part_itr,
                                      operation_type::Select, usertable_ptr,
                                      rec_ptr, -1, NULL, tindx_ptr, projection);

        vector<statement*> stmts = { st };

        transaction txn(stmts);
        load.txns.push_back(txn);
      }
    }

  }

  //cout << load.txns.size() << " workload transactions " << endl;

  return &load;
}
