// LSM FILE

#include "lsm_engine.h"
#include <fstream>

using namespace std;

void lsm_engine::group_commit() {

  while (ready) {
    std::this_thread::sleep_for(std::chrono::milliseconds(conf.gc_interval));

    if (!ready)
      break;

    //std::cout << "Syncing log !" << endl;

    // sync
    fs_log.sync();

  }
}

void lsm_engine::merge() {
  std::cout << "Merging ! " << merge_looper <<endl;

  vector<table*> tables = db->tables->get_data();
  for (table* tab : tables) {
    table_index *table_index = tab->indices->at(0);

    pbtree<unsigned long, record*>* pm_map = table_index->pm_map;

    // Check if need to merge
    if (pm_map->size() > conf.merge_ratio * table_index->off_map->size()) {

      pbtree<unsigned long, record*>::const_iterator itr;
      record *pm_rec, *fs_rec;
      unsigned long key;
      off_t storage_offset;
      std::string val;

      // All tuples in table
      for (itr = pm_map->begin(); itr != pm_map->end(); itr++) {
        key = (*itr).first;
        pm_rec = (*itr).second;

        fs_rec = NULL;

        // Check if we need to merge
        if (table_index->off_map->exists(key) != 0) {
          storage_offset = table_index->off_map->at(key);
          val = tab->fs_data.at(storage_offset);
          fs_rec = deserialize_to_record(val, tab->sptr, false);

          int num_cols = pm_rec->sptr->num_columns;
          for (int field_itr = 0; field_itr < num_cols; field_itr++) {
            fs_rec->set_data(field_itr, pm_rec);
          }

          val = serialize(fs_rec, tab->sptr, false);
          //LOG_INFO("Merge :: update :: val :: %s ", val.c_str());

          tab->fs_data.update(storage_offset, val);

          delete fs_rec;
        } else {
          // Insert tuple
          val = serialize(pm_rec, tab->sptr, false);
          //LOG_INFO("Merge :: insert new :: val :: %s ", val.c_str());

          storage_offset = tab->fs_data.push_back(val);
          table_index->off_map->insert(key, storage_offset);
        }
      }

      // Clear mem table
      table_index->pm_map->clear();
    }
  }

  // Truncate log
  fs_log.truncate();
}

lsm_engine::lsm_engine(const config& _conf)
    : conf(_conf),
      db(conf.db) {

  //for (int i = 0; i < conf.num_executors; i++)
  //  executors.push_back(std::thread(&wal_engine::runner, this));

  vector<table*> tables = db->tables->get_data();
  for (table* tab : tables) {
    std::string table_file_name = conf.fs_path + std::string(tab->table_name);
    tab->fs_data.configure(table_file_name, tab->max_tuple_size);
  }

}

lsm_engine::~lsm_engine() {

  // done = true;
  //for (int i = 0; i < conf.num_executors; i++)
  //  executors[i].join();

}

std::string lsm_engine::select(const statement& st) {
  LOG_INFO("Select");
  std::string val;

  record *rec_ptr = st.rec_ptr;
  record *pm_rec = NULL, *fs_rec = NULL;
  table *tab = db->tables->at(st.table_id);
  table_index *table_index = tab->indices->at(st.table_index_id);

  unsigned long key = hash_fn(st.key);
  off_t storage_offset;

  // Check if key exists in mem
  if (table_index->pm_map->exists(key) != 0) {
    LOG_INFO("Using mem table ");
    pm_rec = table_index->pm_map->at(key);
  }

  // Check if key exists in fs
  if (table_index->off_map->exists(key) != 0) {
    LOG_INFO("Using ss table ");
    storage_offset = table_index->off_map->at(key);
    val = tab->fs_data.at(storage_offset);
    fs_rec = deserialize_to_record(val, tab->sptr, false);
  }

  if (pm_rec != NULL && fs_rec == NULL) {
    // From Memtable
    val = serialize(pm_rec, st.projection, false);
  } else if (pm_rec == NULL && fs_rec != NULL) {
    // From SSTable
    val = serialize(fs_rec, st.projection, false);

    delete fs_rec;
  } else if (pm_rec != NULL && fs_rec != NULL) {
    // Merge
    int num_cols = pm_rec->sptr->num_columns;
    for (int field_itr = 0; field_itr < num_cols; field_itr++) {
      if (pm_rec->sptr->columns[field_itr].enabled)
        fs_rec->set_data(field_itr, pm_rec);
    }

    val = serialize(fs_rec, st.projection, false);
    delete fs_rec;
  }

  LOG_INFO("val : %s", val.c_str());
  //cout << "val : " << val << endl;

  return val;
}

void lsm_engine::insert(const statement& st) {
  LOG_INFO("Insert");
  record* after_rec = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;

  std::string key_str = get_data(after_rec, indices->at(0)->sptr);
  unsigned long key = hash_fn(key_str);

  // Add log entry
  entry_stream.str("");
  entry_stream << st.transaction_id << " " << st.op_type << " " << st.table_id
               << " " << serialize(after_rec, after_rec->sptr, true) << "\n";
  entry_str = entry_stream.str();

  // Add log entry
  fs_log.push_back(entry_str);

  // Add entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = get_data(after_rec, indices->at(index_itr)->sptr);
    key = hash_fn(key_str);

    indices->at(index_itr)->pm_map->insert(key, after_rec);
  }
}

void lsm_engine::remove(const statement& st) {
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
  if (indices->at(0)->pm_map->exists(key) == 0) {
    delete rec_ptr;
    return;
  }

  record* before_rec = indices->at(0)->pm_map->at(key);

  // Add log entry
  entry_stream.str("");
  entry_stream << st.transaction_id << " " << st.op_type << " " << st.table_id
               << " " << serialize(before_rec, before_rec->sptr, true) << "\n";

  entry_str = entry_stream.str();
  fs_log.push_back(entry_str);

  // Remove entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = get_data(rec_ptr, indices->at(index_itr)->sptr);
    key = hash_fn(key_str);

    indices->at(index_itr)->pm_map->erase(key);
  }

  delete before_rec;
}

void lsm_engine::update(const statement& st) {
  LOG_INFO("Update");
  record* rec_ptr = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = db->tables->at(st.table_id)->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;

  std::string key_str = get_data(rec_ptr, indices->at(0)->sptr);
  unsigned long key = hash_fn(key_str);
  off_t log_offset;
  std::string val;
  record* before_rec;

  // Check if key does not exist
  if (indices->at(0)->pm_map->exists(key) == 0) {
    //LOG_INFO("Key not found in mem table %lu ", key);
    before_rec = rec_ptr;

  } else {
    before_rec = indices->at(0)->pm_map->at(key);

    // Update existing record
    for (int field_itr : st.field_ids) {
      void* field_ptr = rec_ptr->get_pointer(field_itr);
      before_rec->set_data(field_itr, rec_ptr);
      delete ((char*) field_ptr);
    }
  }

  entry_stream.str("");
  entry_stream << st.transaction_id << " " << st.op_type << " " << st.table_id
               << " " << serialize(before_rec, before_rec->sptr, true) << "\n";
  entry_str = entry_stream.str();

  // Add log entry
  fs_log.push_back(entry_str);

  // Add entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = get_data(before_rec, indices->at(index_itr)->sptr);
    key = hash_fn(key_str);

    indices->at(index_itr)->pm_map->erase(key);
    indices->at(index_itr)->pm_map->insert(key, before_rec);
  }

}

// RUNNER + LOADER

void lsm_engine::execute(const transaction& txn) {

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

  if (++merge_looper % conf.merge_interval == 0)
    merge();

}

void lsm_engine::runner() {
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

void lsm_engine::generator(const workload& load, bool stats) {

  fs_log.configure(conf.fs_path + "log");
  merge_looper = 0;

  timeval t1, t2;
  gettimeofday(&t1, NULL);

  // Logger start
  std::thread gc(&lsm_engine::group_commit, this);

  for (const transaction& txn : load.txns)
    execute(txn);

  // Logger end
  ready = false;
  gc.join();

  fs_log.sync();
  fs_log.close();

  gettimeofday(&t2, NULL);

  if (stats) {
    cout << "LSM :: ";
    display_stats(t1, t2, conf.num_txns);
  }
}

void lsm_engine::recovery() {
}

