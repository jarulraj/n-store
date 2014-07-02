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
      pmemalloc_activate(vc);
    }

    std::sprintf(&(data[sptr->columns[0].offset]), "%d", _key);
    std::sprintf(&(data[sptr->columns[1].offset]), "%p", vc);
  }

  ~usertable_record() {
    delete vc;
  }

  char* vc;
};

ycsb_benchmark::ycsb_benchmark(config& _conf)
    : conf(_conf),
      txn_id(0) {

  database* db;

  // Initialization mode
  if (conf.sp->init == 0) {
    db = new database();
    conf.sp->ptrs[conf.sp->itr++] = db;
    pmemalloc_activate(db);
    conf.db = db;

    plist<table*>* tables = new plist<table*>(&conf.sp->ptrs[conf.sp->itr++],
                                              &conf.sp->ptrs[conf.sp->itr++]);
    pmemalloc_activate(tables);
    db->tables = tables;

    plist<char*>* log = new plist<char*>(&conf.sp->ptrs[conf.sp->itr++],
                                         &conf.sp->ptrs[conf.sp->itr++]);
    db->log = log;
    pmemalloc_activate(log);

    plist<void*>* commit_free_list = new plist<void*>(
        &conf.sp->ptrs[conf.sp->itr++], &conf.sp->ptrs[conf.sp->itr++]);
    db->commit_free_list = commit_free_list;
    pmemalloc_activate(commit_free_list);

    plist<void*>* recovery_free_list = new plist<void*>(
        &conf.sp->ptrs[conf.sp->itr++], &conf.sp->ptrs[conf.sp->itr++]);
    db->recovery_free_list = recovery_free_list;
    pmemalloc_activate(recovery_free_list);

    // USERTABLE
    size_t offset = 0, len = 0;
    field_info col1(offset, sizeof(int), field_type::INTEGER, 1, 1);
    offset += col1.len;
    field_info col2(offset, sizeof(void*), field_type::VARCHAR, 0, 1);
    offset += col2.len;
    len = offset;

    vector<field_info> cols;
    cols.push_back(col1);
    cols.push_back(col2);
    schema* usertable_schema = new schema(cols, len);
    pmemalloc_activate(usertable_schema);

    table* usertable = new table("usertable", usertable_schema, 1);
    pmemalloc_activate(usertable);
    tables->push_back(usertable);

    plist<table_index*>* indices = new plist<table_index*>(
        &conf.sp->ptrs[conf.sp->itr++], &conf.sp->ptrs[conf.sp->itr++]);
    pmemalloc_activate(indices);
    usertable->indices = indices;

    cols.clear();
    cols.push_back(col1);
    schema* usertable_index_schema = new schema(cols, len);
    pmemalloc_activate(usertable_index_schema);

    table_index* key_index = new table_index(usertable_index_schema, 2);
    pmemalloc_activate(key_index);
    indices->push_back(key_index);

    ptree<unsigned long, record*>* key_index_map = new ptree<unsigned long,
        record*>(&conf.sp->ptrs[conf.sp->itr++]);
    pmemalloc_activate(key_index_map);
    key_index->map = key_index_map;

    cout << "Initialization Mode" << endl;
    conf.sp->init = 1;
  } else {
    cout << "Recovery Mode " << endl;
    db = (database*) conf.sp->ptrs[0];
    conf.db = db;

    cout<<"Map size ::"<<db->tables->at(0)->indices->at(0)->map->size<<endl;
  }

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
  unsigned int transaction_id;
  schema* usertable_schema = conf.db->tables->at(usertable_id)->sptr;

  int part_range = conf.num_keys / conf.num_parts;
  int part_itr, txn_itr, t_id;

  for (int txn_itr = 0; txn_itr < part_range; txn_itr++) {

    for (int part_itr = 0; part_itr < conf.num_parts; part_itr++) {
      t_id = part_range * part_itr + txn_itr;

      transaction_id = ++txn_id;

      // INSERT

      int key = t_id;
      std::string value = random_string(conf.sz_value);

      record* rec_ptr = new usertable_record(usertable_schema, key, value);

      statement st(transaction_id, partition_type::Single, part_itr,
                   operation_type::Insert, usertable_id, rec_ptr, -1,
                   usertable_index_id, NULL);

      vector<statement> stmts = { st };
      transaction txn(transaction_id, stmts);
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
  std::string empty;

  int part_range = conf.num_keys / conf.num_parts;
  int part_txns = conf.num_txns / conf.num_parts;
  int part_itr, txn_itr, t_id;
  unsigned int transaction_id;
  schema* usertable_schema = conf.db->tables->at(usertable_id)->sptr;

  for (txn_itr = 0; txn_itr < part_txns; txn_itr++) {

    for (part_itr = 0; part_itr < conf.num_parts; part_itr++) {
      t_id = part_range * part_itr + txn_itr;

      //int key = conf.zipf_dist[t_id];
      int key = rand() % 10;
      double u = conf.uniform_dist[t_id];

      // UPDATE
      if (u < conf.per_writes) {

        std::string updated_val(conf.sz_value, 'x');

        record* rec_ptr = new usertable_record(usertable_schema, key,
                                               updated_val);

        transaction_id = ++txn_id;

        statement st(transaction_id, partition_type::Single, part_itr,
                     operation_type::Update, usertable_id, rec_ptr, 1,
                     usertable_index_id, NULL);

        vector<statement> stmts = { st };

        transaction txn(transaction_id, stmts);
        load.txns.push_back(txn);
      } else {

        // SELECT

        record* rec_ptr = new usertable_record(usertable_schema, key, empty);

        statement st(transaction_id, partition_type::Single, part_itr,
                     operation_type::Select, usertable_id, rec_ptr, -1,
                     usertable_index_id, usertable_schema);

        vector<statement> stmts = { st };

        transaction txn(transaction_id, stmts);
        load.txns.push_back(txn);
      }
    }

  }

  //cout << load.txns.size() << " workload transactions " << endl;

  return load;
}
