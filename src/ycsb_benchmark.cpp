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
  usertable_record(unsigned int _num_fields, unsigned int _key,
                   const std::string& _val)
      : record(_num_fields) {

    integer_field* key = new integer_field(_key);
    varchar_field* val = new varchar_field(_val);

    data[0] = key;
    data[1] = val;
  }

};

ycsb_benchmark::ycsb_benchmark(config& _conf)
    : conf(_conf),
      s_id(0) {

  // Table 0 : usertable with index on key
  table usertable("usertable");

  vector<bool> key = { 1, 0 };
  table_index key_index(key);

  usertable.indices.push_back(key_index);
  load.tables.push_back(usertable);

  // Generate Zipf dist
  long part_range = conf.num_keys / conf.num_parts;
  long part_txns = conf.num_txns / conf.num_parts;

  zipf(zipf_dist, conf.skew, part_range, part_txns);
  uniform(uniform_dist, part_txns);

}

workload& ycsb_benchmark::get_dataset() {

  load.txns.clear();

  unsigned int usertable_id = 0;
  unsigned int usertable_index_id = 0;
  vector<bool> projection = { 0, 0 };

  int part_range = conf.num_keys / conf.num_parts;
  int part_itr, txn_itr, txn_id;

  for (int txn_itr = 0; txn_itr < part_range; txn_itr++) {

    for (int part_itr = 0; part_itr < conf.num_parts; part_itr++) {
      txn_id = part_range * part_itr + txn_itr;

      int key = txn_id;
      std::string value = random_string(conf.sz_value);

      record* rec_ptr = new usertable_record(2, key, value);

      statement st(++s_id, partition_type::Single, part_itr,
                   operation_type::Insert, usertable_id, rec_ptr, -1, NULL,
                   usertable_index_id, projection);

      vector<statement> stmts = { st };
      transaction txn(stmts);
      load.txns.push_back(txn);
    }

  }

  //cout << load.txns.size() << " dataset transactions " << endl;

  return load;
}

workload& ycsb_benchmark::get_workload() {

  load.txns.clear();

  unsigned int usertable_id = 0;
  unsigned int usertable_index_id = 0;
  vector<bool> projection = { 1, 1 };
  std::string empty;

  int part_range = conf.num_keys / conf.num_parts;
  int part_txns = conf.num_txns / conf.num_parts;
  int part_itr, txn_itr, txn_id;

  for (txn_itr = 0; txn_itr < part_txns; txn_itr++) {

    for (part_itr = 0; part_itr < conf.num_parts; part_itr++) {
      txn_id = part_range * part_itr + txn_itr;

      int key = conf.zipf_dist[txn_id];
      double u = conf.uniform_dist[txn_id];

      // UPDATE
      if (u < conf.per_writes) {

        std::string updated_val(conf.sz_value, 'x');

        varchar_field* val = new varchar_field(updated_val);

        record* rec_ptr = new usertable_record(2, key, empty);

        statement st(++s_id, partition_type::Single, part_itr,
                     operation_type::Update, usertable_id, rec_ptr, 1, val,
                     usertable_index_id, projection);

        vector<statement> stmts = { st };

        transaction txn(stmts);
        load.txns.push_back(txn);
      } else {

        // SELECT

        record* rec_ptr = new usertable_record(2, key, empty);

        statement st(++s_id, partition_type::Single, part_itr,
                     operation_type::Select, usertable_id, rec_ptr, -1, NULL,
                     usertable_index_id, projection);

        vector<statement> stmts = { st };

        transaction txn(stmts);
        load.txns.push_back(txn);
      }
    }

  }

  //cout << load.txns.size() << " workload transactions " << endl;

  return load;
}
