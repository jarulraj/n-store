// OPT LSM

#include "opt_lsm_engine.h"
#include <fstream>

using namespace std;

void opt_lsm_engine::group_commit() {

  while (ready) {
    //std::cout << "Syncing log !" << endl;

    // no need to sync log

    std::this_thread::sleep_for(std::chrono::milliseconds(conf.gc_interval));
  }
}

opt_lsm_engine::opt_lsm_engine(const config& _conf)
    : conf(_conf),
      db(conf.db),
      pm_log(db->log) {

  //for (int i = 0; i < conf.num_executors; i++)
  //  executors.push_back(std::thread(&wal_engine::runner, this));

}

opt_lsm_engine::~opt_lsm_engine() {

  // done = true;
  //for (int i = 0; i < conf.num_executors; i++)
  //  executors[i].join();

}

std::string opt_lsm_engine::select(const statement& st) {
  LOG_INFO("Select");
  std::string val;

  record* rec_ptr = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  table_index* table_index = tab->indices->at(st.table_index_id);

  unsigned long key = hash_fn(st.key);
  //cout << "key :: " << st.key << endl;
  off_t log_offset;

  log_offset = table_index->off_map->at(key);
  val = std::string(pm_log->at(log_offset));
  val = deserialize_to_string(val, st.projection, true);
  LOG_INFO("val : %s", val.c_str());

  return val;
}

void opt_lsm_engine::insert(const statement& st) {
  LOG_INFO("Insert");
  record* after_rec = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;
  off_t log_offset;

  std::string key_str = get_data(after_rec, indices->at(0)->sptr);
  //cout << "key_str :: " << key_str << endl;
  unsigned long key = hash_fn(key_str);

  // Check if key exists
  if (indices->at(0)->off_map->exists(key) != 0) {
    return;
  }

  // Add log entry
  entry_stream.str("");
  entry_stream << st.transaction_id << " " << st.op_type << " " << st.table_id
               << " " << serialize(after_rec, after_rec->sptr, true) << "\n";
  entry_str = entry_stream.str();

  char* entry = new char[entry_str.size() + 1];
  strcpy(entry, entry_str.c_str());
  pmemalloc_activate(entry);
  log_offset = pm_log->push_back(entry);
  //cout << "offset :: " << log_offset << endl;

  // Add entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = get_data(after_rec, indices->at(index_itr)->sptr);
    key = hash_fn(key_str);

    indices->at(index_itr)->off_map->insert(key, log_offset);
  }
}

void opt_lsm_engine::remove(const statement& st) {
  LOG_INFO("Remove");
  record* rec_ptr = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;
  off_t log_offset;
  std::string val;

  std::string key_str = get_data(rec_ptr, indices->at(0)->sptr);
  unsigned long key = hash_fn(key_str);

  // Check if key does not exist
  if (indices->at(0)->off_map->exists(key) == 0) {
    return;
  }

  log_offset = indices->at(0)->off_map->at(key);
  val = std::string(pm_log->at(log_offset));
  record* before_rec = deserialize_to_record(val, st.rec_ptr->sptr, true);

  // Add TOMBSTONE log entry
  entry_stream.str("");
  entry_stream << st.transaction_id << " " << st.op_type << " " << st.table_id
               << " " << serialize(before_rec, before_rec->sptr, true) << "\n";

  entry_str = entry_stream.str();
  char* entry = new char[entry_str.size() + 1];
  strcpy(entry, entry_str.c_str());
  log_offset = pm_log->push_back(entry);

  pmemalloc_activate(entry);
  pm_log->push_back(entry);

  // Remove entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = get_data(rec_ptr, indices->at(index_itr)->sptr);
    key = hash_fn(key_str);

    indices->at(index_itr)->off_map->erase(key);
  }

}

void opt_lsm_engine::update(const statement& st) {
  LOG_INFO("Update");
  record* rec_ptr = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = db->tables->at(st.table_id)->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;

  std::string key_str = get_data(rec_ptr, indices->at(0)->sptr);
  //cout << "key_str :: " << key_str << endl;
  unsigned long key = hash_fn(key_str);
  off_t log_offset;
  std::string val;

  // Check if key does not exist
  if (indices->at(0)->off_map->exists(key) == 0) {
    return;
  }

  log_offset = indices->at(0)->off_map->at(key);
  val = std::string(pm_log->at(log_offset));
  record* before_rec = deserialize_to_record(val, st.rec_ptr->sptr, true);

  std::string after_data, before_data;
  int num_fields = st.field_ids.size();

  for (int field_itr : st.field_ids) {
    // Update existing record
    before_rec->set_data(field_itr, rec_ptr);
  }

  // Add log entry
  entry_stream.str("");
  entry_stream << st.transaction_id << " " << st.op_type << " " << st.table_id
               << " " << serialize(before_rec, before_rec->sptr, true) << "\n";
  entry_str = entry_stream.str();

  char* entry = new char[entry_str.size() + 1];
  strcpy(entry, entry_str.c_str());
  pmemalloc_activate(entry);
  log_offset = pm_log->push_back(entry);
  //cout << "offset :: " << log_offset << endl;

  // Add entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = get_data(before_rec, indices->at(index_itr)->sptr);
    key = hash_fn(key_str);

    indices->at(index_itr)->off_map->update(key, log_offset);
  }

}

// RUNNER + LOADER

void opt_lsm_engine::execute(const transaction& txn) {

  for (const statement& st : txn.stmts) {
    if (st.op_type == operation_type::Select) {
      select(st);
    } else if (st.op_type == operation_type::Insert) {
      insert(st);
    } else if (st.op_type == operation_type::Update) {
      update(st);
    } else if (st.op_type == operation_type::Delete) {
      remove(st);
    }
  }

}

void opt_lsm_engine::runner() {
  bool empty = true;

  while (!done) {
    rdlock(&txn_queue_rwlock);
    empty = txn_queue.empty();
    unlock(&txn_queue_rwlock);

    if (!empty) {
      wrlock(&txn_queue_rwlock);
      const transaction& txn = txn_queue.front();
      txn_queue.pop();
      unlock(&txn_queue_rwlock);

      execute(txn);
    }
  }

  while (!txn_queue.empty()) {
    wrlock(&txn_queue_rwlock);
    const transaction& txn = txn_queue.front();
    txn_queue.pop();
    unlock(&txn_queue_rwlock);

    execute(txn);
  }
}

void opt_lsm_engine::generator(const workload& load, bool stats) {

  timeval t1, t2;
  gettimeofday(&t1, NULL);

// Logger start
  std::thread gc(&opt_lsm_engine::group_commit, this);
  ready = true;

  for (const transaction& txn : load.txns)
    execute(txn);

// Logger end
  ready = false;
  gc.join();

  gettimeofday(&t2, NULL);

  if (stats) {
    cout << "OPT LSM :: ";
    display_stats(t1, t2, conf.num_txns);
  }
}

void opt_lsm_engine::recovery() {

  int op_type, txn_id, table_id;
  table *tab;
  plist<table_index*>* indices;
  unsigned int num_indices, index_itr;

  field_info finfo;
  std::string entry_str;
  off_t log_offset;

  vector<char*> pm_log_data = pm_log->get_data();
  for (char* ptr : pm_log_data) {
    //LOG_INFO("entry : %s ", entry_str.c_str());
    std::stringstream entry(ptr);

    entry >> txn_id >> op_type >> table_id;

    switch (op_type) {
      case operation_type::Insert: {
        LOG_INFO("Redo Insert");

        tab = db->tables->at(table_id);
        schema* sptr = tab->sptr;
        record* after_rec = deserialize_to_record(entry_str, sptr, true);

        indices = tab->indices;
        num_indices = tab->num_indices;

        // Add entry in indices
        for (index_itr = 0; index_itr < num_indices; index_itr++) {
          std::string key_str = get_data(after_rec,
                                         indices->at(index_itr)->sptr);
          unsigned long key = hash_fn(key_str);

          indices->at(index_itr)->off_map->insert(key, log_offset);
        }
      }
        break;

      case operation_type::Delete: {
        LOG_INFO("Redo Delete");

        tab = db->tables->at(table_id);
        schema* sptr = tab->sptr;
        record* before_rec = deserialize_to_record(entry_str, sptr, true);

        indices = tab->indices;
        num_indices = tab->num_indices;

        // Remove entry in indices
        for (index_itr = 0; index_itr < num_indices; index_itr++) {
          std::string key_str = get_data(before_rec,
                                         indices->at(index_itr)->sptr);
          unsigned long key = hash_fn(key_str);

          indices->at(index_itr)->off_map->erase(key);
        }
      }
        break;

      case operation_type::Update:
        LOG_INFO("Redo Update");
        {
          int num_fields;
          int field_itr;

          tab = db->tables->at(table_id);
          schema* sptr = tab->sptr;
          record* before_rec = deserialize_to_record(entry_str, sptr, true);
          record* after_rec = deserialize_to_record(entry_str, sptr, true);

          // Update entry in indices
          for (index_itr = 0; index_itr < num_indices; index_itr++) {
            std::string key_str = get_data(before_rec,
                                           indices->at(index_itr)->sptr);
            unsigned long key = hash_fn(key_str);

            indices->at(index_itr)->off_map->insert(key, log_offset);
          }

        }

        break;

      default:
        cout << "Invalid operation type" << op_type << endl;
        break;
    }

  }

}

