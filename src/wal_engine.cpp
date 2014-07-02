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
  std::string val;
  record* rec_ptr = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  table_index* table_index = tab->indices->at(st.table_index_id);

  std::string key_str = get_data(rec_ptr, table_index->sptr);
  unsigned long key = hash_fn(key_str);

  // check if key does not exist
  if (table_index->map->contains(key) == 0) {
    return val;
  }

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

  std::string key_str = get_data(after_rec, indices->at(index_itr)->sptr);
  unsigned long key = hash_fn(key_str);

  // Check if key exists
  if (indices->at(0)->map->contains(key) != 0) {
    return;
  }

  // Add log entry
  entry_stream.str("");
  entry_stream << st.transaction_id << " " << st.op_type << " " << st.table_id
               << " " << after_rec << endl;
  entry_str = entry_stream.str();
  entry_len = entry_str.size();

  char* entry = new char[entry_len + 1];
  strcpy(entry, entry_str.c_str());
  pmemalloc_activate(entry);
  undo_log->push_back(entry);

  // Activate new record
  db->recovery_free_list->push_back(after_rec);
  pmemalloc_activate(after_rec);
  after_rec->persist_data();

  // Add entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    std::string key_str = get_data(after_rec, indices->at(index_itr)->sptr);
    unsigned long key = hash_fn(key_str);

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

  record* before_rec = indices->at(index_itr)->map->at(key);
  db->commit_free_list->push_back(before_rec);

  // Add log entry
  entry_stream.str("");
  entry_stream << st.transaction_id << " " << st.op_type << " " << st.table_id
               << " " << before_rec << endl;
  entry_str = entry_stream.str();
  entry_len = entry_str.size();

  char* entry = new char[entry_len + 1];
  strcpy(entry, entry_str.c_str());
  pmemalloc_activate(entry);
  undo_log->push_back(entry);

  // Remove entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    std::string key_str = get_data(rec_ptr, indices->at(index_itr)->sptr);
    unsigned long key = hash_fn(key_str);

    indices->at(index_itr)->map->erase(key);
  }

}

void wal_engine::update(const statement& st) {
  //cout<<"Update "<<endl;
  record* rec_ptr = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;
  void *before_field, *after_field;

  unsigned int index_itr = 0;
  int field_id = st.field_id;

  std::string key_str = get_data(rec_ptr, indices->at(index_itr)->sptr);
  unsigned long key = hash_fn(key_str);

  // Check if key does not exist
  if (indices->at(index_itr)->map->contains(key) == 0) {
    return;
  }

  record* before_rec = indices->at(index_itr)->map->at(key);
  before_field = before_rec->get_pointer(field_id, before_rec->sptr);
  after_field = rec_ptr->get_pointer(field_id, rec_ptr->sptr);

  // Add log entry
  entry_stream.str("");
  entry_stream << st.transaction_id << " " << st.op_type << " " << st.table_id
               << " " << before_rec << " " << before_field << endl;
  entry_str = entry_stream.str();
  entry_len = entry_str.size();

  char* entry = new char[entry_len + 1];
  strcpy(entry, entry_str.c_str());
  pmemalloc_activate(entry);
  undo_log->push_back(entry);

  // Activate new field and garbage collect previous field
  if (rec_ptr->sptr->columns[field_id].inlined == 0) {
    pmemalloc_activate(after_field);
    db->commit_free_list->push_back(before_field);
  }

  // Update existing record
  before_rec->set_data(field_id, rec_ptr, before_rec->sptr);
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

  // Clean up stuff
  vector<void*> commit_free_list = db->commit_free_list->get_data();
  for (void* ptr : commit_free_list) {
    pmemalloc_free(ptr);
  }
  db->commit_free_list->clear();

}

void wal_engine::runner() {
  bool empty = true;

  while (!done) {
    rdlock (&txn_queue_rwlock);
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
    wrlock (&txn_queue_rwlock);
    const transaction& txn = txn_queue.front();
    txn_queue.pop();
    unlock(&txn_queue_rwlock);

    execute(txn);
  }
}

void wal_engine::generator(const workload& load) {

  recovery();

  for (const transaction& txn : load.txns)
    execute(txn);

}

void wal_engine::recovery() {

  vector<char*> undo_log = db->log->get_data();

  for (char* ptr : undo_log) {
    std::string str(ptr);
    delete ptr;
  }

  db->log->clear();
}

