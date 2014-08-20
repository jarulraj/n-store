// LSM - FILE BASED

#include "lsm_engine.h"
#include <fstream>

using namespace std;

lsm_engine::lsm_engine(const config& _conf, database* _db, bool _read_only,
                       unsigned int _tid)
    : conf(_conf),
      db(_db),
      tid(_tid) {

  etype = engine_type::LSM;
  read_only = _read_only;
  fs_log.configure(conf.fs_path + std::to_string(_tid) + "_" + "log");
  merge_looper = 0;

  vector<table*> tables = db->tables->get_data();
  for (table* tab : tables) {
    std::string table_file_name = conf.fs_path + std::to_string(_tid) + "_"
        + std::string(tab->table_name);
    tab->fs_data.configure(table_file_name, tab->max_tuple_size, false);
  }

  // GC start
  if (!read_only) {
    gc = std::thread(&lsm_engine::group_commit, this);
  }
}

lsm_engine::~lsm_engine() {
  // GC end
  if (!read_only) {
    ready = false;
    gc.join();

    merge(true);

    if (!conf.recovery) {
      fs_log.sync();

      if(conf.storage_stats)
        fs_log.truncate_chunk();
      else
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

  record*rec_ptr = st.rec_ptr;
  record *pm_rec = NULL, *fs_rec = NULL;
  table* tab = db->tables->at(st.table_id);
  table_index* table_index = tab->indices->at(st.table_index_id);
  std::string key_str = sr.serialize(rec_ptr, table_index->sptr);

  unsigned long key = hash_fn(key_str);
  bool fs_storage = false;
  off_t storage_offset = 0;

  // Check if key exists in mem
  table_index->pm_map->at(key, &pm_rec);

  // Check if key exists in fs
  fs_storage = table_index->off_map->at(key, &storage_offset);

  if (fs_storage) {
    val = tab->fs_data.at(storage_offset);
    if (!val.empty())
      fs_rec = sr.deserialize(val, tab->sptr);
  }

  if (pm_rec != NULL && fs_rec == NULL) {
    val = sr.serialize(pm_rec, st.projection);
  } else if (pm_rec == NULL && fs_rec != NULL) {
    val = sr.serialize(fs_rec, st.projection);

    fs_rec->clear_data();
    delete fs_rec;
  } else if (pm_rec != NULL && fs_rec != NULL) {
    // Merge
    int num_cols = pm_rec->sptr->num_columns;
    for (int field_itr = 0; field_itr < num_cols; field_itr++) {
      if (pm_rec->sptr->columns[field_itr].enabled)
        fs_rec->set_data(field_itr, pm_rec);
    }

    val = sr.serialize(fs_rec, st.projection);
    delete fs_rec;
  }

  LOG_INFO("val : %s", val.c_str());
  //cout << "val : " << val << endl;

  delete rec_ptr;
  return val;
}

int lsm_engine::insert(const statement& st) {
  LOG_INFO("Insert");
  record* after_rec = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;

  std::string key_str = sr.serialize(after_rec, indices->at(0)->sptr);
  unsigned long key = hash_fn(key_str);

  // Check if key exists
  if (indices->at(0)->pm_map->exists(key)
      || indices->at(0)->off_map->exists(key)) {
    after_rec->clear_data();
    delete after_rec;
    return EXIT_SUCCESS;
  }

  // Add log entry
  entry_stream.str("");
  entry_stream << st.transaction_id << " " << st.op_type << " " << st.table_id
               << " " << sr.serialize(after_rec, after_rec->sptr) << "\n";
  entry_str = entry_stream.str();

  // Add log entry
  fs_log.push_back(entry_str);

  // Add entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = sr.serialize(after_rec, indices->at(index_itr)->sptr);
    key = hash_fn(key_str);

    indices->at(index_itr)->pm_map->insert(key, after_rec);
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
  std::string val;

  std::string key_str = sr.serialize(rec_ptr, indices->at(0)->sptr);
  //LOG_INFO("Key_str :: --%s-- ", key_str.c_str());
  unsigned long key = hash_fn(key_str);

  // Check if key does not exist
  if (indices->at(0)->pm_map->exists(key) == 0
      && indices->at(0)->off_map->exists(key) == 0) {
    delete rec_ptr;
    return EXIT_SUCCESS;
  }

  record* before_rec = NULL;
  indices->at(0)->pm_map->at(key, &before_rec);

  // Add log entry
  entry_stream.str("");
  entry_stream << st.transaction_id << " " << st.op_type << " " << st.table_id
               << " " << sr.serialize(rec_ptr, rec_ptr->sptr) << "\n";

  entry_str = entry_stream.str();
  fs_log.push_back(entry_str);

  // Remove entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = sr.serialize(rec_ptr, indices->at(index_itr)->sptr);
    key = hash_fn(key_str);

    indices->at(index_itr)->pm_map->erase(key);
    indices->at(index_itr)->off_map->erase(key);
  }

  before_rec->clear_data();
  delete before_rec;
  return EXIT_SUCCESS;
}

int lsm_engine::update(const statement& st) {
  LOG_INFO("Update");
  record* rec_ptr = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = db->tables->at(st.table_id)->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;

  std::string key_str = sr.serialize(rec_ptr, indices->at(0)->sptr);
  unsigned long key = hash_fn(key_str);
  std::string val;
  record* before_rec = NULL;
  void *before_field;

  entry_stream.str("");

  // Check if key does not exist
  if (indices->at(0)->pm_map->at(key, &before_rec) == false) {
    before_rec = rec_ptr;

    entry_stream << st.transaction_id << " " << st.op_type << " " << st.table_id
                 << " " << sr.serialize(before_rec, before_rec->sptr) << " ";

    entry_stream << sr.serialize(before_rec, before_rec->sptr) << "\n";
    entry_str = entry_stream.str();

    for (index_itr = 0; index_itr < num_indices; index_itr++) {
      key_str = sr.serialize(before_rec, indices->at(index_itr)->sptr);
      key = hash_fn(key_str);

      indices->at(index_itr)->pm_map->insert(key, before_rec);
    }
  } else {
    entry_stream << st.transaction_id << " " << st.op_type << " " << st.table_id
                 << " " << sr.serialize(before_rec, before_rec->sptr) << " ";

    // Update existing record
    for (int field_itr : st.field_ids) {
      if (rec_ptr->sptr->columns[field_itr].inlined == 0) {
        before_field = before_rec->get_pointer(field_itr);
        delete ((char*) before_field);
      }

      before_rec->set_data(field_itr, rec_ptr);
    }

    entry_stream << sr.serialize(before_rec, before_rec->sptr) << "\n";
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

  std::string key_str = sr.serialize(after_rec, indices->at(0)->sptr);
  unsigned long key = hash_fn(key_str);

  if (!conf.recovery) {
    // Add log entry
    entry_stream.str("");
    entry_stream << st.transaction_id << " " << st.op_type << " " << st.table_id
                 << " " << sr.serialize(after_rec, after_rec->sptr) << "\n";
    entry_str = entry_stream.str();

    // Add log entry
    fs_log.push_back(entry_str);
  }

  // Add entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = sr.serialize(after_rec, indices->at(index_itr)->sptr);
    key = hash_fn(key_str);

    indices->at(index_itr)->pm_map->insert(key, after_rec);
  }
}

void lsm_engine::group_commit() {

  while (ready) {
    // sync
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

  vector<table*> tables = db->tables->get_data();
  for (table* tab : tables) {
    table_index *p_index = tab->indices->at(0);
    vector<table_index*> indices = tab->indices->get_data();

    pbtree<unsigned long, record*>* pm_map = p_index->pm_map;

    size_t compact_threshold = conf.merge_ratio * p_index->off_map->size();
    bool compact = (pm_map->size() > compact_threshold);

    // Check if need to merge
    if (force || compact) {
      //std::cout << "Merging ! " << endl;
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

          if (!val.empty()) {
            fs_rec = sr.deserialize(val, tab->sptr);

            int num_cols = pm_rec->sptr->num_columns;
            for (int field_itr = 0; field_itr < num_cols; field_itr++) {
              fs_rec->set_data(field_itr, pm_rec);
            }

            val = sr.serialize(fs_rec, tab->sptr);
            //LOG_INFO("Merge :: update :: val :: %s ", val.c_str());

            tab->fs_data.update(storage_offset, val);
            fs_rec->clear_data();
            delete fs_rec;
          }
        } else {
          // Insert tuple
          val = sr.serialize(pm_rec, tab->sptr);
          //LOG_INFO("Merge :: insert new :: val :: %s ", val.c_str());

          storage_offset = tab->fs_data.push_back(val);

          for (table_index* index : indices) {
            std::string key_str = sr.serialize(pm_rec, index->sptr);
            key = hash_fn(key_str);
            index->off_map->insert(key, storage_offset);
          }
        }

        //pm_rec->clear_data();
        delete pm_rec;
      }

      // Clear mem table
      for (table_index* index : indices)
        index->pm_map->clear();
    }
  }

  // Truncate log
  //if (force)
  //  fs_log.truncate();
}

void lsm_engine::txn_begin() {
}

void lsm_engine::txn_end(__attribute__((unused)) bool commit) {
  if (read_only)
    return;

  merge_check();
}

void lsm_engine::recovery() {

  LOG_INFO("LSM recovery");

  // Setup recovery
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

  int entry_itr = 0;
  while (std::getline(log_file, entry_str)) {
    entry_itr++;
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
        if (!undo_mode) {
          LOG_INFO("Redo Insert");
        } else {
          LOG_INFO("Undo Delete");
        }

        tab = db->tables->at(table_id);
        schema* sptr = tab->sptr;

        tuple_str = get_tuple(entry, sptr);
        record* after_rec = sr.deserialize(tuple_str, sptr);
        st = statement(0, operation_type::Insert, table_id, after_rec);
        insert(st);
      }
        break;

      case operation_type::Delete: {
        if (!undo_mode) {
          LOG_INFO("Redo Delete");
        } else {
          LOG_INFO("Undo Insert");
        }

        tab = db->tables->at(table_id);
        schema* sptr = tab->sptr;

        tuple_str = get_tuple(entry, sptr);
        record* before_rec = sr.deserialize(tuple_str, sptr);
        st = statement(0, operation_type::Delete, table_id, before_rec);
        remove(st);
      }
        break;

      case operation_type::Update: {
        if (!undo_mode) {
          LOG_INFO("Redo Update");
        } else {
          LOG_INFO("Undo Update");
        }

        tab = db->tables->at(table_id);
        schema* sptr = tab->sptr;
        tuple_str = get_tuple(entry, sptr);
        record* before_rec = sr.deserialize(tuple_str, sptr);
        tuple_str = get_tuple(entry, sptr);
        record* after_rec = sr.deserialize(tuple_str, sptr);

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
  cout << "entries :: " << entry_itr << endl;

}

