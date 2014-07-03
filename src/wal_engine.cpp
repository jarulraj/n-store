// WRITE-AHEAD LOGGING

#include "wal_engine.h"

using namespace std;

wal_engine::wal_engine(const config& _conf)
    : conf(_conf),
      db(conf.db),
      undo_log(db->log),
      entry_len(0) {

  //for (int i = 0; i < conf.num_executors; i++)
  //  executors.push_back(std::thread(&wal_engine::runner, this));

}

wal_engine::~wal_engine() {

  // done = true;
  //for (int i = 0; i < conf.num_executors; i++)
  //  executors[i].join();

}

std::string wal_engine::select(const statement& st) {
  record* rec_ptr = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  table_index* table_index = tab->indices->at(st.table_index_id);

  unsigned long key = hash_fn(st.key);
  std::string val;

  rec_ptr = table_index->map->at(key);
  val = get_data(rec_ptr, st.projection);
  //cout << "val :" << val << endl;

  return val;
}

void wal_engine::insert(const statement& st) {
  record* after_rec = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;

  std::string key_str = get_data(after_rec, indices->at(0)->sptr);
  unsigned long key = hash_fn(key_str);

  // Check if key exists
  if (indices->at(0)->map->contains(key) != 0) {
    return;
  }

  // Add log entry
  char* entry = new char[MAX_ENTRY_LEN];
  std::sprintf(entry, "%d %d %d %p \n", st.transaction_id, st.op_type,
               st.table_id, after_rec);
  pmemalloc_activate(entry);
  undo_log->push_back(entry);

  // Activate new record
  pmemalloc_activate(after_rec);
  after_rec->persist_data();

  // Add entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = get_data(after_rec, indices->at(index_itr)->sptr);
    key = hash_fn(key_str);

    indices->at(index_itr)->map->insert(key, after_rec);
  }
}

void wal_engine::remove(const statement& st) {
  record* rec_ptr = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;

  std::string key_str = get_data(rec_ptr, indices->at(0)->sptr);
  unsigned long key = hash_fn(key_str);

  // Check if key does not exist
  if (indices->at(0)->map->contains(key) == 0) {
    return;
  }

  record* before_rec = indices->at(0)->map->at(key);
  db->commit_free_list->push_back(before_rec);

  // Add log entry
  char* entry = new char[MAX_ENTRY_LEN];
  std::sprintf(entry, "%d %d %d %p \n", st.transaction_id, st.op_type,
               st.table_id, before_rec);
  pmemalloc_activate(entry);
  undo_log->push_back(entry);

  // Remove entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = get_data(rec_ptr, indices->at(index_itr)->sptr);
    key = hash_fn(key_str);

    indices->at(index_itr)->map->erase(key);
  }

}

void wal_engine::update(const statement& st) {
  record* rec_ptr = st.rec_ptr;
  plist<table_index*>* indices = db->tables->at(st.table_id)->indices;

  std::string key_str = get_data(rec_ptr, indices->at(0)->sptr);
  unsigned long key = hash_fn(key_str);

  record* before_rec = indices->at(0)->map->at(key);

  // Check if key does not exist
  if (before_rec == 0)
    return;

  void *before_field, *after_field;
  int field_id = st.field_id;
  char* entry = new char[MAX_ENTRY_LEN];

  // Pointer field
  if (rec_ptr->sptr->columns[field_id].inlined == 0) {
    before_field = before_rec->get_pointer(field_id);
    after_field = rec_ptr->get_pointer(field_id);

    std::sprintf(entry, "%d %d %d %d %p %p %p \n", st.transaction_id,
                 st.op_type, st.table_id, field_id, before_rec, before_field,
                 after_field);
  }
  // Data field
  else {
    before_field = before_rec->get_pointer(field_id);
    std::string before_data = before_rec->get_data(field_id);

    std::sprintf(entry, "%d %d %d %d %p %s \n", st.transaction_id, st.op_type,
                 st.table_id, field_id, before_rec, before_data.c_str());
  }

  // Add log entry
  pmemalloc_activate(entry);
  undo_log->push_back(entry);

  // Activate new field and garbage collect previous field
  if (rec_ptr->sptr->columns[field_id].inlined == 0) {
    db->commit_free_list->push_back(before_field);
    pmemalloc_activate(after_field);
  }

  // Update existing record
  before_rec->set_data(field_id, rec_ptr);
}

// RUNNER + LOADER

void wal_engine::execute(const transaction& txn) {
  vector<statement>::const_iterator stmt_itr;

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

  // Clear commit_free list
  vector<void*> commit_free_list = db->commit_free_list->get_data();
  for (void* ptr : commit_free_list) {
    pmemalloc_free(ptr);
  }
  db->commit_free_list->clear();

  // Clear log
  vector<char*> undo_log = db->log->get_data();
  for (char* ptr : undo_log)
    delete ptr;
  db->log->clear();
}

void wal_engine::runner() {
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

void wal_engine::generator(const workload& load) {

  recovery();

  timespec time1, time2;
  clock_gettime(CLOCK_REALTIME, &time1);
  for (const transaction& txn : load.txns)
    execute(txn);
  clock_gettime(CLOCK_REALTIME, &time2);

  display_stats(time1, time2, load.txns.size());
}

void wal_engine::recovery() {
  vector<char*> undo_vec = db->log->get_data();

  int op_type, txn_id, table_id;
  table *tab;
  plist<table_index*>* indices;
  unsigned int num_indices, index_itr;
  record *before_rec, *after_rec;
  field_info finfo;

  for (char* ptr : undo_vec) {
    std::sscanf(ptr, "%d %d %d", &txn_id, &op_type, &table_id);

    switch (op_type) {
      case operation_type::Insert:
        std::sscanf(ptr, "%d %d %d %p", &txn_id, &op_type, &table_id,
                    &after_rec);
        cout << "after_rec :: " << after_rec << endl;

        tab = db->tables->at(table_id);
        indices = tab->indices;
        num_indices = tab->num_indices;

        // Remove entry in indices
        for (index_itr = 0; index_itr < num_indices; index_itr++) {
          std::string key_str = get_data(after_rec,
                                         indices->at(index_itr)->sptr);
          unsigned long key = hash_fn(key_str);

          indices->at(index_itr)->map->erase(key);
        }

        // Free after_rec
        pmemalloc_free(after_rec);
        break;

      case operation_type::Delete:
        std::sscanf(ptr, "%d %d %d %p", &txn_id, &op_type, &table_id,
                    &before_rec);

        tab = db->tables->at(table_id);
        indices = tab->indices;
        num_indices = tab->num_indices;

        // Fix entry in indices to point to before_rec
        for (index_itr = 0; index_itr < num_indices; index_itr++) {
          std::string key_str = get_data(before_rec,
                                         indices->at(index_itr)->sptr);
          unsigned long key = hash_fn(key_str);

          indices->at(index_itr)->map->insert(key, before_rec);
        }
        break;

      case operation_type::Update:
        int field_id;
        std::sscanf(ptr, "%d %d %d %d %p", &txn_id, &op_type, &table_id,
                    &field_id, &before_rec);

        tab = db->tables->at(table_id);
        indices = tab->indices;
        finfo = before_rec->sptr->columns[field_id];

        // Pointer
        if (finfo.inlined == 0) {
          void *before_field, *after_field;
          std::sscanf(ptr, "%d %d %d %d %p %p %p", &txn_id, &op_type, &table_id,
                      &field_id, &before_rec, &before_field, &after_field);
          before_rec->set_pointer(field_id, before_field);

          // Free after_field
          pmemalloc_free(after_field);
        }
        // Data
        else {
          field_type type = finfo.type;
          size_t offset = before_rec->sptr->columns[field_id].offset;

          switch (type) {
            case field_type::INTEGER:
              int ival;
              std::sscanf(ptr, "%d %d %d %d %p %d", &txn_id, &op_type,
                          &table_id, &field_id, &before_rec, &ival);
              std::sprintf(&(before_rec->data[offset]), "%d", ival);
              break;

            case field_type::DOUBLE:
              double dval;
              std::sscanf(ptr, "%d %d %d %d %p %lf", &txn_id, &op_type,
                          &table_id, &field_id, &before_rec, &dval);
              std::sprintf(&(before_rec->data[offset]), "%lf", dval);
              break;

            default:
              cout << "Invalid field type" << op_type << endl;
              break;
          }
        }
        break;

      default:
        cout << "Invalid operation type" << op_type << endl;
        break;
    }

    delete ptr;
  }

  db->log->clear();
}

