// SP

#include "sp_engine.h"

using namespace std;

void sp_engine::group_commit() {

  while (ready) {
    unsigned int version;

    wrlock(&ptreap_rwlock);
    db->dirs->next_version();
    version = db->dirs->version;
    unlock(&ptreap_rwlock);

    db->version = (version-1);
    if(version > 1)
      db->dirs->delete_versions(version-2);

    std::this_thread::sleep_for(std::chrono::milliseconds(conf.gc_interval));
  }
}

sp_engine::sp_engine(const config& _conf)
    : conf(_conf),
      db(conf.db) {

  //for (int i = 0; i < conf.num_executors; i++)
  //  executors.push_back(std::thread(&wal_engine::runner, this));

}

sp_engine::~sp_engine() {

  // done = true;
  //for (int i = 0; i < conf.num_executors; i++)
  //  executors[i].join();

}

std::string sp_engine::select(const statement& st) {
  LOG_INFO("Select");
  record* rec_ptr = st.rec_ptr;
  std::string table_id_str = std::to_string(st.table_id);
  std::string table_index_id_str = std::to_string(st.table_index_id);
  unsigned int version = db->version;

  unsigned long key = hash_fn(table_id_str + table_index_id_str + st.key);
  std::string val;

  // Read from latest clean version
  rec_ptr = db->dirs->at(key, version);

  val = get_data(rec_ptr, st.projection);
  LOG_INFO("val : %s", val.c_str());

  return val;
}

void sp_engine::insert(const statement& st) {
  LOG_INFO("Insert");
  record* after_rec = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  std::string table_id_str = std::to_string(st.table_id);
  std::string table_index_id_str = std::to_string(0);

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;

  std::string key_str = get_data(after_rec, indices->at(0)->sptr);
  unsigned long key = hash_fn(table_id_str + table_index_id_str + key_str);

  // Check if key exists in current version
  if (db->dirs->at(key) != 0) {
    return;
  }

  // Activate new record
  pmemalloc_activate(after_rec);
  after_rec->persist_data();

  // Add entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    table_index_id_str = std::to_string(index_itr);
    key_str = get_data(after_rec, indices->at(index_itr)->sptr);
    key = hash_fn(table_id_str + table_index_id_str + key_str);

    wrlock(&ptreap_rwlock);
    db->dirs->insert(key, after_rec);
    unlock(&ptreap_rwlock);
  }
}

void sp_engine::remove(const statement& st) {
  LOG_INFO("Remove");
  record* rec_ptr = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  std::string table_id_str = std::to_string(st.table_id);
  std::string table_index_id_str = std::to_string(0);

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;

  std::string key_str = get_data(rec_ptr, indices->at(0)->sptr);
  unsigned long key = hash_fn(table_id_str + table_index_id_str + key_str);

  // Check if key does not exist in current version
  if (db->dirs->at(key) == 0) {
    return;
  }

  // Record will be freed when all references are gone
  //record* before_rec = db->dirs->at(key);

  // Remove entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    table_index_id_str = std::to_string(index_itr);
    key_str = get_data(rec_ptr, indices->at(index_itr)->sptr);
    key = hash_fn(table_id_str + table_index_id_str + key_str);

    wrlock(&ptreap_rwlock);
    db->dirs->remove(key);
    unlock(&ptreap_rwlock);
  }

}

void sp_engine::update(const statement& st) {
  LOG_INFO("Update");
  record* rec_ptr = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  std::string table_id_str = std::to_string(st.table_id);
  std::string table_index_id_str = std::to_string(0);

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;

  std::string key_str = get_data(rec_ptr, indices->at(0)->sptr);
  unsigned long key = hash_fn(table_id_str + table_index_id_str + key_str);

  // Read from current version
  record* before_rec = db->dirs->at(key);

  // Check if key does not exist in current version
  if (before_rec == 0)
    return;

  record* after_rec = new record(before_rec->sptr);
  memcpy(after_rec->data, before_rec->data, before_rec->data_len);

  void *before_field, *after_field;
  int num_fields = st.field_ids.size();

  for (int field_itr : st.field_ids) {
    // Update new record
    after_rec->set_data(field_itr, rec_ptr);
  }

  // Activate new record
  pmemalloc_activate(after_rec);
  after_rec->persist_data();

  // Update entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    table_index_id_str = std::to_string(index_itr);
    key_str = get_data(after_rec, indices->at(index_itr)->sptr);
    key = hash_fn(table_id_str + table_index_id_str + key_str);

    wrlock(&ptreap_rwlock);
    db->dirs->insert(key, after_rec);
    unlock(&ptreap_rwlock);
  }
}

// RUNNER + LOADER

void sp_engine::execute(const transaction& txn) {

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

  // Sync what ?

}

void sp_engine::runner() {
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

void sp_engine::generator(const workload& load, bool stats) {

  std::thread gc(&sp_engine::group_commit, this);
  ready = true;

  for (const transaction& txn : load.txns)
    execute(txn);

  ready = false;
  gc.join();

}

