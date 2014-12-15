// WAL LOGGING

#include "wal_engine.h"

namespace storage {

wal_engine::wal_engine(const config& _conf, database* _db, bool _read_only,
                       unsigned int _tid)
    : conf(_conf),
      db(_db),
      tid(_tid) {
  etype = engine_type::WAL;
  read_only = _read_only;
  fs_log.configure(conf.fs_path + std::to_string(_tid) + "_" + "log");

  std::vector<table*> tables = db->tables->get_data();
  for (table* tab : tables) {
	std::string table_file_name = conf.fs_path + std::to_string(_tid) + "_"
			+ std::string(tab->table_name);
	tab->fs_data.configure(table_file_name, tab->max_tuple_size, false);
  }

  // Logger start
  if (!read_only) {
    gc = std::thread(&wal_engine::group_commit, this);
    ready = true;
  }
}

wal_engine::~wal_engine() {

  // Logger end
  if (!read_only) {
    ready = false;
    gc.join();

    if (!conf.recovery) {
      fs_log.sync();
      fs_log.close();
    }

    std::vector<table*> tables = db->tables->get_data();
    for (table* tab : tables) {
      tab->fs_data.sync();
      tab->fs_data.close();
    }
  }

}

std::string wal_engine::select(const statement& st) {
  LOG_INFO("Select");
  record* rec_ptr = st.rec_ptr;
  record* select_ptr = NULL;
  table* tab = db->tables->at(st.table_id);
  table_index* table_index = tab->indices->at(st.table_index_id);
  std::string key_str = sr.serialize(rec_ptr, table_index->sptr);

  unsigned long key = hash_fn(key_str);
  std::string val;

  table_index->pm_map->at(key, &select_ptr);
  if (select_ptr)
    val = sr.serialize(select_ptr, st.projection);
  LOG_INFO("val : %s", val.c_str());

  delete rec_ptr;
  return val;
}

int wal_engine::insert(const statement& st) {
  LOG_INFO("Insert");
  record* after_rec = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;

  std::string key_str = sr.serialize(after_rec, indices->at(0)->sptr);
  LOG_INFO("key_str :: %s", key_str.c_str());
  unsigned long key = hash_fn(key_str);

  // Check if key present
  if (indices->at(0)->pm_map->exists(key) != 0) {
    after_rec->clear_data();
    delete after_rec;
    return EXIT_SUCCESS;
  }

  // Add log entry
  std::string after_tuple = sr.serialize(after_rec, after_rec->sptr);
  entry_stream.str("");
  entry_stream << st.transaction_id << " " << st.op_type << " " << st.table_id
               << " " << after_tuple << "\n";
  entry_str = entry_stream.str();
  fs_log.push_back(entry_str);

  // Add to table
  tab->pm_data->push_back(after_rec);

  off_t storage_offset;
  storage_offset = tab->fs_data.push_back(after_tuple);

  // Add entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = sr.serialize(after_rec, indices->at(index_itr)->sptr);
    key = hash_fn(key_str);

    indices->at(index_itr)->pm_map->insert(key, after_rec);
    indices->at(index_itr)->off_map->insert(key, storage_offset);
  }

  return EXIT_SUCCESS;
}

int wal_engine::remove(const statement& st) {
  LOG_INFO("Remove");
  record* rec_ptr = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;
  record* before_rec = NULL;

  std::string key_str = sr.serialize(rec_ptr, indices->at(0)->sptr);
  unsigned long key = hash_fn(key_str);

  // Check if key does not exist
  if (indices->at(0)->pm_map->at(key, &before_rec) == false) {
	delete rec_ptr;
    return EXIT_SUCCESS;
  }

  // Add log entry
  entry_stream.str("");
  entry_stream << st.transaction_id << " " << st.op_type << " " << st.table_id
               << " " << sr.serialize(before_rec, before_rec->sptr) << "\n";

  entry_str = entry_stream.str();
  fs_log.push_back(entry_str);

  tab->pm_data->erase(before_rec);

  // Remove entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = sr.serialize(rec_ptr, indices->at(index_itr)->sptr);
    key = hash_fn(key_str);

    indices->at(index_itr)->pm_map->erase(key);
    indices->at(index_itr)->off_map->erase(key);
  }

  before_rec->clear_data();
  delete before_rec;

  delete rec_ptr;
  return EXIT_SUCCESS;
}

int wal_engine::update(const statement& st) {
  LOG_INFO("Update");
  record* rec_ptr = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = db->tables->at(st.table_id)->indices;

  std::string key_str = sr.serialize(rec_ptr, indices->at(0)->sptr);
  unsigned long key = hash_fn(key_str);
  record* before_rec;

  // Check if key does not exist
  if (indices->at(0)->pm_map->at(key, &before_rec) == false) {
	rec_ptr->clear_data();
    delete rec_ptr;
    return EXIT_SUCCESS;
  }

  entry_stream.str("");
  entry_stream << st.transaction_id << " " << st.op_type << " " << st.table_id
               << " ";
  entry_stream << sr.serialize(before_rec, tab->sptr) << " ";

  // Update existing record
  for (int field_itr : st.field_ids) {
    if (rec_ptr->sptr->columns[field_itr].inlined == 0) {
      void* before_field = before_rec->get_pointer(field_itr);
      delete (char*) before_field;
    }

    before_rec->set_data(field_itr, rec_ptr);
  }

  std::string before_tuple;
  before_tuple = sr.serialize(before_rec, tab->sptr);
  entry_stream << before_tuple  << "\n";

  // Add log entry
  entry_str = entry_stream.str();
  fs_log.push_back(entry_str);

  off_t storage_offset = 0;
  indices->at(0)->off_map->at(key, &storage_offset);
  tab->fs_data.update(storage_offset, before_tuple);

  delete rec_ptr;
  return EXIT_SUCCESS;
}

void wal_engine::txn_begin() {
}

void wal_engine::txn_end(__attribute__((unused)) bool commit) {
}

void wal_engine::load(const statement& st) {
  //LOG_INFO("Load");
  record* after_rec = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;

  std::string key_str = sr.serialize(after_rec, indices->at(0)->sptr);
  unsigned long key = hash_fn(key_str);

  std::string after_tuple = sr.serialize(after_rec, after_rec->sptr);

  // Add log entry
  if (!conf.recovery) {
    entry_stream.str("");
    entry_stream << st.transaction_id << " " << st.op_type << " " << st.table_id
                 << " " << after_tuple << "\n";
    entry_str = entry_stream.str();
    fs_log.push_back(entry_str);
  }

  tab->pm_data->push_back(after_rec);

  off_t storage_offset;
  storage_offset = tab->fs_data.push_back(after_tuple);

  // Add entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = sr.serialize(after_rec, indices->at(index_itr)->sptr);
    key = hash_fn(key_str);

    indices->at(index_itr)->pm_map->insert(key, after_rec);
    indices->at(index_itr)->off_map->insert(key, storage_offset);
  }

}

void wal_engine::group_commit() {

  while (ready) {
    // sync
    fs_log.sync();

    std::this_thread::sleep_for(std::chrono::milliseconds(conf.gc_interval));
  }
}

void wal_engine::recovery() {

  LOG_INFO("WAL recovery");

  // Setup recovery
  fs_log.sync();
  fs_log.disable();

  // Clear off_map and rebuild it
  std::vector<table*> tables = db->tables->get_data();
  for (table* tab : tables) {
    std::vector<table_index*> indices = tab->indices->get_data();

    for (table_index* index : indices) {
      index->off_map->clear();
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
  log_file.seekg(0, std::ios::beg);

  int entry_itr = 0;
  while (std::getline(log_file, entry_str)) {
    entry_itr++;
    //std::cout << "entry :  " << entry_str.c_str() << std::endl;
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
        std::cout << "Invalid operation type" << op_type << std::endl;
        break;
    }

  }

  rec_t.end();
  std::cout << "WAL :: Recovery duration (ms) : " << rec_t.duration()
            << std::endl;
  std::cout << "entries :: " << entry_itr << std::endl;
}

}
