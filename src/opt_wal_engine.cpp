// OPT WRITE-AHEAD LOGGING

#include "opt_wal_engine.h"

using namespace std;

opt_wal_engine::opt_wal_engine(const config& _conf, database* _db,
                               bool _read_only, unsigned int _tid)
    : conf(_conf),
      db(_db),
      tid(_tid) {

  etype = engine_type::OPT_WAL;
  read_only = _read_only;
  pm_log = db->log;

}

opt_wal_engine::~opt_wal_engine() {

}

std::string opt_wal_engine::select(const statement& st) {
  LOG_INFO("Select");
  record* rec_ptr = st.rec_ptr;
  record* select_ptr = NULL;
  table* tab = db->tables->at(st.table_id);
  table_index* table_index = tab->indices->at(st.table_index_id);
  std::string key_str = serialize(rec_ptr, table_index->sptr);

  unsigned long key = hash_fn(key_str);
  std::string val;

  table_index->pm_map->at(key, &select_ptr);
  if (select_ptr)
    val = serialize(select_ptr, st.projection);
  LOG_INFO("val : %s", val.c_str());

  delete rec_ptr;
  return val;
}

int opt_wal_engine::insert(const statement& st) {
  //LOG_INFO("Insert");
  record* after_rec = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;

  std::string key_str = serialize(after_rec, indices->at(0)->sptr);
  unsigned long key = hash_fn(key_str);

  // Check if key exists
  if (indices->at(0)->pm_map->exists(key) != 0) {
    delete after_rec;
    return EXIT_SUCCESS;
  }

  // Add log entry
  entry_stream.str("");
  entry_stream << st.transaction_id << " " << st.op_type << " " << st.table_id
               << " " << after_rec;

  entry_str = entry_stream.str();
  size_t entry_str_sz = entry_str.size() + 1;
  char* entry = new char[entry_str_sz];
  memcpy(entry, entry_str.c_str(), entry_str_sz);
  pmemalloc_activate(entry);
  pm_log->push_back(entry);

  // Activate new record
  pmemalloc_activate(after_rec);
  after_rec->persist_data();

  tab->pm_data->push_back(after_rec);

  // Add entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = serialize(after_rec, indices->at(index_itr)->sptr);
    key = hash_fn(key_str);

    indices->at(index_itr)->pm_map->insert(key, after_rec);
  }

  return EXIT_SUCCESS;
}

int opt_wal_engine::remove(const statement& st) {
  LOG_INFO("Remove");
  record* rec_ptr = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;

  std::string key_str = serialize(rec_ptr, indices->at(0)->sptr);
  unsigned long key = hash_fn(key_str);
  record* before_rec = NULL;

  // Check if key does not exist
  if (indices->at(0)->pm_map->at(key, &before_rec) == false) {
    delete rec_ptr;
    return EXIT_SUCCESS;
  }

  int num_cols = before_rec->sptr->num_columns;

  for (int field_itr = 0; field_itr < num_cols; field_itr++) {
    if (before_rec->sptr->columns[field_itr].inlined == 0) {
      void* before_field = before_rec->get_pointer(field_itr);
      commit_free_list.push_back(before_field);
    }
  }
  commit_free_list.push_back(before_rec);

  // Add log entry
  entry_stream.str("");
  entry_stream << st.transaction_id << " " << st.op_type << " " << st.table_id
               << " " << before_rec;

  entry_str = entry_stream.str();
  size_t entry_str_sz = entry_str.size() + 1;
  char* entry = new char[entry_str_sz];
  memcpy(entry, entry_str.c_str(), entry_str_sz);

  pmemalloc_activate(entry);
  pm_log->push_back(entry);

  tab->pm_data->erase(before_rec);

  // Remove entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = serialize(rec_ptr, indices->at(index_itr)->sptr);
    key = hash_fn(key_str);

    indices->at(index_itr)->pm_map->erase(key);
  }

  delete rec_ptr;
  return EXIT_SUCCESS;
}

int opt_wal_engine::update(const statement& st) {
  LOG_INFO("Update");
  record* rec_ptr = st.rec_ptr;
  plist<table_index*>* indices = db->tables->at(st.table_id)->indices;

  std::string key_str = serialize(rec_ptr, indices->at(0)->sptr);
  unsigned long key = hash_fn(key_str);
  record* before_rec;

  // Check if key does not exist
  if (indices->at(0)->pm_map->at(key, &before_rec) == false) {
    delete rec_ptr;
    return EXIT_SUCCESS;
  }

  void *before_field, *after_field;
  int num_fields = st.field_ids.size();

  entry_stream.str("");
  entry_stream << st.transaction_id << " " << st.op_type << " " << st.table_id
               << " " << num_fields << " " << before_rec << " ";

  for (int field_itr : st.field_ids) {
    // Pointer field
    if (rec_ptr->sptr->columns[field_itr].inlined == 0) {
      before_field = before_rec->get_pointer(field_itr);
      after_field = rec_ptr->get_pointer(field_itr);

      entry_stream << field_itr << " " << before_field << " ";
    }
    // Data field
    else {
      std::string before_data = before_rec->get_data(field_itr);

      entry_stream << field_itr << " " << " " << before_data << " ";
    }
  }

  // Add log entry
  entry_str = entry_stream.str();
  size_t entry_str_sz = entry_str.size() + 1;
  char* entry = new char[entry_str_sz];
  memcpy(entry, entry_str.c_str(), entry_str_sz);

  pmemalloc_activate(entry);
  pm_log->push_back(entry);

  for (int field_itr : st.field_ids) {
    // Activate new field and garbage collect previous field
    if (rec_ptr->sptr->columns[field_itr].inlined == 0) {
      before_field = before_rec->get_pointer(field_itr);
      after_field = rec_ptr->get_pointer(field_itr);

      pmemalloc_activate(after_field);
      commit_free_list.push_back(before_field);
    }

    // Update existing record
    before_rec->set_data(field_itr, rec_ptr);
  }

  delete rec_ptr;
  return EXIT_SUCCESS;
}

void opt_wal_engine::load(const statement& st) {
  //LOG_INFO("Load");
  record* after_rec = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;
  std::string key_str = serialize(after_rec, indices->at(0)->sptr);
  unsigned long key = hash_fn(key_str);

  // Add log entry
  entry_stream.str("");
  entry_stream << st.transaction_id << " " << st.op_type << " " << st.table_id
               << " " << after_rec;

  entry_str = entry_stream.str();
  size_t entry_str_sz = entry_str.size() + 1;
  char* entry = new char[entry_str_sz];
  memcpy(entry, entry_str.c_str(), entry_str_sz);
  pmemalloc_activate(entry);
  pm_log->push_back(entry);

  // Activate new record
  pmemalloc_activate(after_rec);
  after_rec->persist_data();

  tab->pm_data->push_back(after_rec);

  // Add entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = serialize(after_rec, indices->at(index_itr)->sptr);
    key = hash_fn(key_str);

    indices->at(index_itr)->pm_map->insert(key, after_rec);
  }

}

void opt_wal_engine::txn_begin() {
}

void opt_wal_engine::txn_end(__attribute__((unused)) bool commit) {
  // Clear commit_free list
  for (void* ptr : commit_free_list) {
    pmemalloc_free(ptr);
  }
  commit_free_list.clear();

  // Clear log
  vector<char*> undo_log = pm_log->get_data();
  for (char* ptr : undo_log)
    delete ptr;
  pm_log->clear();
}

void opt_wal_engine::recovery() {

  LOG_INFO("OPT WAL recovery");

  vector<char*> undo_log = pm_log->get_data();

  int op_type, txn_id, table_id;
  unsigned int num_indices, index_itr;
  table *tab;
  plist<table_index*>* indices;

  std::string ptr_str;
  record *before_rec, *after_rec;
  field_info finfo;

  timer rec_t;
  rec_t.start();

  for (char* ptr : undo_log) {
    //cout << "entry : --" << ptr << "-- " << endl;
    std::stringstream entry(ptr);

    entry >> txn_id >> op_type >> table_id;

    switch (op_type) {
      case operation_type::Insert:
        LOG_INFO("Undo Insert");
        entry >> ptr_str;
        std::sscanf(ptr_str.c_str(), "%p", &after_rec);

        tab = db->tables->at(table_id);
        indices = tab->indices;
        num_indices = tab->num_indices;

        tab->pm_data->erase(after_rec);

        // Remove entry in indices
        for (index_itr = 0; index_itr < num_indices; index_itr++) {
          std::string key_str = serialize(after_rec,
                                          indices->at(index_itr)->sptr);
          unsigned long key = hash_fn(key_str);

          indices->at(index_itr)->pm_map->erase(key);
        }

        // Free after_rec
        after_rec->clear_data();
        delete after_rec;
        break;

      case operation_type::Delete:
        LOG_INFO("Undo Delete");
        entry >> ptr_str;
        std::sscanf(ptr_str.c_str(), "%p", &before_rec);

        tab = db->tables->at(table_id);
        indices = tab->indices;
        num_indices = tab->num_indices;

        tab->pm_data->push_back(after_rec);

        // Fix entry in indices to point to before_rec
        for (index_itr = 0; index_itr < num_indices; index_itr++) {
          std::string key_str = serialize(before_rec,
                                          indices->at(index_itr)->sptr);
          unsigned long key = hash_fn(key_str);

          indices->at(index_itr)->pm_map->insert(key, before_rec);
        }
        break;

      case operation_type::Update:
        LOG_INFO("Undo Update");
        int num_fields;
        int field_itr;

        entry >> num_fields >> ptr_str;
        std::sscanf(ptr_str.c_str(), "%p", &before_rec);
        //printf("before rec :: --%p-- \n", before_rec);

        for (field_itr = 0; field_itr < num_fields; field_itr++) {
          entry >> field_itr;

          tab = db->tables->at(table_id);
          indices = tab->indices;
          finfo = before_rec->sptr->columns[field_itr];

          // Pointer
          if (finfo.inlined == 0) {
            LOG_INFO("Pointer ");
            void *before_field, *after_field;

            entry >> ptr_str;
            std::sscanf(ptr_str.c_str(), "%p", &before_field);

            after_field = before_rec->get_pointer(field_itr);
            before_rec->set_pointer(field_itr, before_field);

            // Free after_field
            delete ((char*) after_field);
          }
          // Data
          else {
            LOG_INFO("Inlined ");
            field_type type = finfo.type;

            switch (type) {
              case field_type::INTEGER:
                int ival;
                entry >> ival;
                before_rec->set_int(field_itr, ival);
                break;

              case field_type::DOUBLE:
                double dval;
                entry >> dval;
                before_rec->set_int(field_itr, dval);
                break;

              default:
                cout << "Invalid field type : " << op_type << endl;
                break;
            }
          }
        }
        break;

      default:
        cout << "Invalid operation type" << op_type << endl;
        break;
    }

    delete ptr;
  }

  // Clear log
  pm_log->clear();

  rec_t.end();
  cout << "OPT_WAL :: Recovery duration (ms) : " << rec_t.duration() << endl;

}

