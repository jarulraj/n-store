// LSM - FILE BASED

#include "lsm_engine.h"
#include <fstream>

using namespace std;

void lsm_engine::group_commit() {

  while (ready) {
    //std::cout << "Syncing ! " << endl;

    // sync
    if (tid == 0)
      fs_log.sync();

    std::this_thread::sleep_for(std::chrono::milliseconds(conf.gc_interval));
  }
}

void lsm_engine::merge_check() {
  if (++merge_looper % conf.merge_interval == 0) {
    merge(false);
    merge_looper = 0;
  }
}

void lsm_engine::merge(bool force) {
  //std::cout << "Merging ! " << endl;

  wrlock(&db->engine_rwlock);

  vector<table*> tables = db->tables->get_data();
  for (table* tab : tables) {
    table_index *p_index = tab->indices->at(0);
    vector<table_index*> indices = tab->indices->get_data();

    pbtree<unsigned long, record*>* pm_map = p_index->pm_map;

    size_t compact_threshold = conf.merge_ratio * p_index->off_map->size();
    bool compact = (pm_map->size() > compact_threshold);

    // Check if need to merge
    if (force || compact) {

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
        if (p_index->off_map->at(key, &storage_offset)) {
          val = tab->fs_data.at(storage_offset);
          fs_rec = deserialize(val, tab->sptr);

          int num_cols = pm_rec->sptr->num_columns;
          for (int field_itr = 0; field_itr < num_cols; field_itr++) {
            fs_rec->set_data(field_itr, pm_rec);
          }

          val = serialize(fs_rec, tab->sptr);
          //LOG_INFO("Merge :: update :: val :: %s ", val.c_str());

          tab->fs_data.update(storage_offset, val);

          delete fs_rec;

        } else {
          // Insert tuple
          val = serialize(pm_rec, tab->sptr);
          //LOG_INFO("Merge :: insert new :: val :: %s ", val.c_str());

          storage_offset = tab->fs_data.push_back(val);

          for (table_index* index : indices) {
            std::string key_str = serialize(pm_rec, index->sptr);
            key = hash_fn(key_str);
            index->off_map->insert(key, storage_offset);
          }
        }

        delete pm_rec;
      }

      // Clear mem table
      for (table_index* index : indices)
        index->pm_map->clear();
    }
  }

  unlock(&db->engine_rwlock);

  // Truncate log
  //if (force)
  //  fs_log.truncate();
}

lsm_engine::lsm_engine(const config& _conf, bool _read_only, unsigned int _tid)
    : conf(_conf),
      db(conf.db),
      tid(_tid) {

  etype = engine_type::LSM;
  read_only = _read_only;
  fs_log.configure(conf.fs_path + "log");
  merge_looper = 0;

  if (tid == 0) {
    vector<table*> tables = db->tables->get_data();
    for (table* tab : tables) {
      std::string table_file_name = conf.fs_path + std::string(tab->table_name);
      tab->fs_data.configure(table_file_name, tab->max_tuple_size, false);
    }
  }

  // Logger start
  if (!read_only && tid == 0) {
    gc = std::thread(&lsm_engine::group_commit, this);
  }
}

lsm_engine::~lsm_engine() {
  // Logger end
  if (!read_only && tid == 0) {
    ready = false;
    gc.join();

    merge(true);

    if (!conf.recovery) {
      fs_log.sync();
      fs_log.close();
    }

    vector<table*> tables = db->tables->get_data();
    for (table* tab : tables) {
      tab->fs_data.sync();
      tab->fs_data.close();
    }
  }
}

std::string lsm_engine::select(const statement& st) {
  LOG_INFO("Select");
  std::string val;

  record *rec_ptr = st.rec_ptr;
  record *pm_rec = NULL, *fs_rec = NULL;
  table *tab = db->tables->at(st.table_id);
  table_index *table_index = tab->indices->at(st.table_index_id);
  std::string key_str = serialize(rec_ptr, table_index->sptr);

  unsigned long key = hash_fn(key_str);
  off_t storage_offset;

  // Check if key exists in mem
  rdlock(&table_index->index_rwlock);
  if ((table_index->pm_map->at(key, &pm_rec))) {
    LOG_INFO("Using mem table ");
    //printf("pm_rec :: %p \n", pm_rec);
  }

  // Check if key exists in fs
  if (table_index->off_map->at(key, &storage_offset)) {
    LOG_INFO("Using ss table ");
    val = tab->fs_data.at(storage_offset);
    fs_rec = deserialize(val, tab->sptr);
    //printf("fs_rec :: %p \n", fs_rec);
  }

  if (pm_rec != NULL && fs_rec == NULL) {
    // From Memtable
    val = serialize(pm_rec, st.projection);
  } else if (pm_rec == NULL && fs_rec != NULL) {
    // From SSTable
    val = serialize(fs_rec, st.projection);

    delete fs_rec;
  } else if (pm_rec != NULL && fs_rec != NULL) {
    // Merge
    int num_cols = pm_rec->sptr->num_columns;
    for (int field_itr = 0; field_itr < num_cols; field_itr++) {
      if (pm_rec->sptr->columns[field_itr].enabled)
        fs_rec->set_data(field_itr, pm_rec);
    }

    val = serialize(fs_rec, st.projection);
    delete fs_rec;
  }
  unlock(&table_index->index_rwlock);

  LOG_INFO("val : %s", val.c_str());
  //cout << "val : " << val << endl;

  return val;
}

int lsm_engine::insert(const statement& st) {
  LOG_INFO("Insert");
  record* after_rec = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;

  std::string key_str = serialize(after_rec, indices->at(0)->sptr);
  //LOG_INFO("Key_str :: --%s-- ", key_str.c_str());
  unsigned long key = hash_fn(key_str);

  // Check if key exists
  rdlock(&indices->at(0)->index_rwlock);
  if (indices->at(0)->pm_map->exists(key)
      || indices->at(0)->off_map->exists(key)) {
    delete after_rec;
    unlock(&indices->at(0)->index_rwlock);
    return EXIT_SUCCESS;
  }
  unlock(&indices->at(0)->index_rwlock);

  // Add log entry
  entry_stream.str("");
  entry_stream << st.transaction_id << " " << st.op_type << " " << st.table_id
               << " " << serialize(after_rec, after_rec->sptr) << "\n";
  entry_str = entry_stream.str();

  // Add log entry
  fs_log.push_back(entry_str);

  // Add entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = serialize(after_rec, indices->at(index_itr)->sptr);
    key = hash_fn(key_str);

    wrlock(&indices->at(index_itr)->index_rwlock);
    indices->at(index_itr)->pm_map->insert(key, after_rec);
    unlock(&indices->at(index_itr)->index_rwlock);
  }

  return EXIT_SUCCESS;
}

int lsm_engine::remove(const statement& st) {
  LOG_INFO("Remove");
  record* rec_ptr = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;
  off_t log_offset;
  std::string val;

  std::string key_str = serialize(rec_ptr, indices->at(0)->sptr);
  //LOG_INFO("Key_str :: --%s-- ", key_str.c_str());
  unsigned long key = hash_fn(key_str);

  // Check if key does not exist
  rdlock(&indices->at(0)->index_rwlock);
  if (indices->at(0)->pm_map->exists(key) == 0
      && indices->at(0)->off_map->exists(key) == 0) {
    LOG_INFO("not found in either index ");
    delete rec_ptr;
    unlock(&indices->at(0)->index_rwlock);
    return EXIT_SUCCESS;
  }
  unlock(&indices->at(0)->index_rwlock);

  // Add log entry
  entry_stream.str("");
  entry_stream << st.transaction_id << " " << st.op_type << " " << st.table_id
               << " " << serialize(rec_ptr, rec_ptr->sptr) << "\n";

  entry_str = entry_stream.str();
  fs_log.push_back(entry_str);

  record* before_rec;
  rdlock(&indices->at(0)->index_rwlock);
  if (indices->at(0)->pm_map->at(key, &before_rec)) {
    // FIXME delete before_rec;
  }
  unlock(&indices->at(0)->index_rwlock);

  // Remove entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = serialize(rec_ptr, indices->at(index_itr)->sptr);
    key = hash_fn(key_str);

    wrlock(&indices->at(index_itr)->index_rwlock);
    indices->at(index_itr)->pm_map->erase(key);
    indices->at(index_itr)->off_map->erase(key);
    unlock(&indices->at(index_itr)->index_rwlock);
  }

  return EXIT_SUCCESS;
}

int lsm_engine::update(const statement& st) {
  LOG_INFO("Update");
  record* rec_ptr = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = db->tables->at(st.table_id)->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;

  std::string key_str = serialize(rec_ptr, indices->at(0)->sptr);
  unsigned long key = hash_fn(key_str);
  off_t log_offset;
  std::string val;
  record* before_rec;
  bool existing_rec = false;
  void *before_field, *after_field;

  entry_stream.str("");

  // Check if key does not exist
  rdlock(&indices->at(0)->index_rwlock);
  if (indices->at(0)->pm_map->at(key, &before_rec) == false) {
    unlock(&indices->at(0)->index_rwlock);

    //LOG_INFO("Key not found in mem table %lu ", key);
    before_rec = rec_ptr;

    entry_stream << st.transaction_id << " " << st.op_type << " " << st.table_id
                 << " " << serialize(before_rec, before_rec->sptr) << " ";

    entry_stream << serialize(before_rec, before_rec->sptr) << "\n";
    entry_str = entry_stream.str();

    for (index_itr = 0; index_itr < num_indices; index_itr++) {
      key_str = serialize(before_rec, indices->at(index_itr)->sptr);
      key = hash_fn(key_str);

      wrlock(&indices->at(0)->index_rwlock);
      indices->at(index_itr)->pm_map->insert(key, before_rec);
      unlock(&indices->at(0)->index_rwlock);
    }
  } else {
    existing_rec = true;
    unlock(&indices->at(0)->index_rwlock);

    entry_stream << st.transaction_id << " " << st.op_type << " " << st.table_id
                 << " " << serialize(before_rec, before_rec->sptr) << " ";

    wrlock(&indices->at(0)->index_rwlock);
    // Update existing record
    for (int field_itr : st.field_ids) {
      if (rec_ptr->sptr->columns[field_itr].inlined == 0) {
        before_field = before_rec->get_pointer(field_itr);
        delete ((char*) before_field);
      }

      before_rec->set_data(field_itr, rec_ptr);
    }
    unlock(&indices->at(0)->index_rwlock);

    entry_stream << serialize(before_rec, before_rec->sptr) << "\n";
    entry_str = entry_stream.str();
  }

  // Add log entry
  fs_log.push_back(entry_str);

  return EXIT_SUCCESS;
}

void lsm_engine::load(const statement& st) {
  //LOG_INFO("Load");
  record* after_rec = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;

  std::string key_str = serialize(after_rec, indices->at(0)->sptr);
  unsigned long key = hash_fn(key_str);

  // Add entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = serialize(after_rec, indices->at(index_itr)->sptr);
    key = hash_fn(key_str);

    indices->at(index_itr)->pm_map->insert(key, after_rec);
  }
}

void lsm_engine::txn_begin() {
  if (!read_only) {
    rdlock(&db->engine_rwlock);
  }
}

void lsm_engine::txn_end(bool commit) {
  if (!read_only) {
    unlock(&db->engine_rwlock);
  }

  if (!read_only && tid == 0)
    merge_check();
}

void lsm_engine::recovery() {

  LOG_INFO("LSM recovery");

  // Setup recovery
  fs_log.flush();
  fs_log.sync();
  fs_log.disable();

  // Clear pm map and rebuild it
  vector<table*> tables = db->tables->get_data();
  for (table* tab : tables) {
    vector<table_index*> indices = tab->indices->get_data();
    for (table_index* index : indices) {
      index->pm_map->clear();
    }
  }

  int op_type, txn_id, table_id;
  std::string entry_str, tuple_str;
  table* tab;
  statement st;
  bool undo_mode = false;

  timer rec_t;
  rec_t.start();

  std::ifstream log_file(fs_log.log_file_name);
  int total_txns = std::count(std::istreambuf_iterator<char>(log_file),
                              std::istreambuf_iterator<char>(), '\n');
  log_file.clear();
  log_file.seekg(0, ios::beg);

  while (std::getline(log_file, entry_str)) {
    //cout << "entry :  " << entry_str.c_str() << endl;
    std::stringstream entry(entry_str);

    entry >> txn_id >> op_type >> table_id;

    if (undo_mode || (total_txns - txn_id < conf.active_txn_threshold)) {
      undo_mode = true;

      switch (op_type) {
        case operation_type::Insert:
          op_type = operation_type::Delete;
          break;
        case operation_type::Delete:
          op_type = operation_type::Insert;
          break;
      }
    }

    switch (op_type) {
      case operation_type::Insert: {
        if (!undo_mode)
          LOG_INFO("Redo Insert");
          else
          LOG_INFO("Undo Delete");

        tab = db->tables->at(table_id);
        schema* sptr = tab->sptr;

        tuple_str = get_tuple(entry, sptr);
        record* after_rec = deserialize(tuple_str, sptr);
        st = statement(0, operation_type::Insert, table_id, after_rec);
        insert(st);
      }
        break;

      case operation_type::Delete: {
        if (!undo_mode)
          LOG_INFO("Redo Delete");
          else
          LOG_INFO("Undo Insert");

        tab = db->tables->at(table_id);
        schema* sptr = tab->sptr;

        tuple_str = get_tuple(entry, sptr);
        record* before_rec = deserialize(tuple_str, sptr);
        st = statement(0, operation_type::Delete, table_id, before_rec);
        remove(st);
      }
        break;

      case operation_type::Update: {
        if (!undo_mode)
          LOG_INFO("Redo Update");
          else
          LOG_INFO("Undo Update");

        tab = db->tables->at(table_id);
        schema* sptr = tab->sptr;
        tuple_str = get_tuple(entry, sptr);
        record* before_rec = deserialize(tuple_str, sptr);
        tuple_str = get_tuple(entry, sptr);
        record* after_rec = deserialize(tuple_str, sptr);

        if (!undo_mode) {
          st = statement(0, operation_type::Delete, table_id, before_rec);
          remove(st);
          st = statement(0, operation_type::Insert, table_id, after_rec);
          insert(st);
        } else {
          st = statement(0, operation_type::Delete, table_id, after_rec);
          remove(st);
          st = statement(0, operation_type::Insert, table_id, before_rec);
          insert(st);
        }
      }

        break;

      default:
        cout << "Invalid operation type" << op_type << endl;
        break;
    }

  }

  fs_log.close();

  rec_t.end();
  cout << "LSM :: Recovery duration (ms) : " << rec_t.duration() << endl;

}

