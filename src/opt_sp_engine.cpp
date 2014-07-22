// OPT SP - does not use files

#include "opt_sp_engine.h"

using namespace std;

void opt_sp_engine::group_commit() {

  while (ready) {

    if (txn_ptr != NULL) {
      wrlock(&opt_sp_pbtree_rwlock);
      assert(bt->txn_commit(txn_ptr) == BT_SUCCESS);

      txn_ptr = bt->txn_begin(0);
      assert(txn_ptr);
      unlock(&opt_sp_pbtree_rwlock);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(conf.gc_interval));
  }
}

opt_sp_engine::opt_sp_engine(const config& _conf)
    : conf(_conf),
      db(conf.db),
      bt(NULL),
      txn_ptr(NULL) {

  //for (int i = 0; i < conf.num_executors; i++)
  //  executors.push_back(std::thread(&wal_engine::runner, this));

}

opt_sp_engine::~opt_sp_engine() {

  // done = true;
  //for (int i = 0; i < conf.num_executors; i++)
  //  executors[i].join();

}

std::string opt_sp_engine::select(const statement& st) {
  LOG_INFO("Select");
  record* rec_ptr = st.rec_ptr;
  struct cow_btval key, val;

  //cout << "select key :: -" << st.key << "-  -" << st.table_id << "-" << st.table_index_id << "-" << endl;
  unsigned long key_id = hasher(hash_fn(st.key), st.table_id,
                                st.table_index_id);
  string key_str = std::to_string(key_id);
  key.data = (void*) key_str.c_str();
  key.size = key_str.size();
  std::string value;
  //cout << "select key :: -" << st.key << "-  -" << key_str << "-" << endl;

  // Read from latest clean version
  if (bt->at(txn_ptr, &key, &val) != BT_FAIL) {
    std::sscanf((char*) val.data, "%p", &rec_ptr);
    value = get_data(rec_ptr, st.projection);
    LOG_INFO("val : %s", value.c_str());
  }

  return value;
}

void opt_sp_engine::insert(const statement& st) {
  //LOG_INFO("Insert");
  record* after_rec = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;
  struct cow_btval key, val;
  val.data = new char[64];

  std::string key_str = get_data(after_rec, indices->at(0)->sptr);
  //cout << "insert key :: -" << key_str << "-  -" << st.table_id << "-0-"<< endl;
  unsigned long key_id = hasher(hash_fn(key_str), st.table_id, 0);
  key_str = std::to_string(key_id);
  //cout << "insert key :: -" << key_str << "- " << endl;

  key.data = (void*) key_str.c_str();
  key.size = key_str.size();

  // Check if key exists in current version
  if (bt->at(txn_ptr, &key, &val) != BT_FAIL) {
    return;
  }

  // Activate new record
  pmemalloc_activate(after_rec);
  after_rec->persist_data();

  // Add entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = get_data(after_rec, indices->at(index_itr)->sptr);
    key_id = hasher(hash_fn(key_str), st.table_id, index_itr);
    key_str = std::to_string(key_id);

    key.data = (void*) key_str.c_str();
    key.size = key_str.size();
    std::sprintf((char*) val.data, "%p", after_rec);
    val.size = strlen((char*) val.data) + 1;
    bt->insert(txn_ptr, &key, &val);
  }

}

void opt_sp_engine::remove(const statement& st) {
  LOG_INFO("Remove");
  record* rec_ptr = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;
  struct cow_btval key, val;

  std::string key_str = get_data(rec_ptr, indices->at(0)->sptr);
  unsigned long key_id = hasher(hash_fn(st.key), st.table_id, 0);
  key_str = std::to_string(key_id);

  key.data = (void*) key_str.c_str();
  key.size = key_str.size();

  // Check if key does not exist
  if (bt->at(txn_ptr, &key, &val) == BT_FAIL) {
    return;
  }

  // Free record
  record* before_rec = new record(rec_ptr->sptr);
  std::sscanf((char*) val.data, "%p", &before_rec);
  delete before_rec;

  // Remove entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = get_data(rec_ptr, indices->at(index_itr)->sptr);
    key_id = hasher(hash_fn(st.key), st.table_id, index_itr);
    key_str = std::to_string(key_id);

    key.data = (void*) key_str.c_str();
    key.size = key_str.size();

    bt->remove(txn_ptr, &key, NULL);
  }

}

void opt_sp_engine::update(const statement& st) {
  LOG_INFO("Update");
  record* rec_ptr = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;
  struct cow_btval key, val, update_val;

  std::string key_str = get_data(rec_ptr, indices->at(0)->sptr);
  unsigned long key_id = hasher(hash_fn(key_str), st.table_id, 0);
  //cout << "update key :: -" << key_str << "-  -" << st.table_id << "-0-" << endl;

  key_str = std::to_string(key_id);

  key.data = (void*) key_str.c_str();
  key.size = key_str.size();
  //cout << "update key :: -" << key_str << endl;

  // Check if key does not exist in current version
  if (bt->at(txn_ptr, &key, &val) == BT_FAIL) {
    return;
  }

  // Read from current version
  record* before_rec = new record(rec_ptr->sptr);
  std::sscanf((char*) val.data, "%p", &before_rec);

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

  update_val.data = new char[64];
  // Update entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = get_data(after_rec, indices->at(index_itr)->sptr);
    key_id = hasher(hash_fn(key_str), st.table_id, index_itr);
    key_str = std::to_string(key_id);

    key.data = (void*) key_str.c_str();
    key.size = key_str.size();

    std::sprintf((char*) update_val.data, "%p", after_rec);
    update_val.size = strlen((char*) val.data) + 1;
    bt->insert(txn_ptr, &key, &update_val);
  }

}

// RUNNER + LOADER

void opt_sp_engine::execute(const transaction& txn) {

  rdlock(&opt_sp_pbtree_rwlock);

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

  unlock(&opt_sp_pbtree_rwlock);

}

void opt_sp_engine::runner() {
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

void opt_sp_engine::generator(const workload& load, bool stats) {

  std::thread gc(&opt_sp_engine::group_commit, this);
  ready = true;

  bt = db->dirs->t_ptr;
  txn_ptr = bt->txn_begin(0);
  assert(txn_ptr);

  struct timeval t1, t2;
  gettimeofday(&t1, NULL);

  for (const transaction& txn : load.txns)
    execute(txn);

  ready = false;
  gc.join();

  if (txn_ptr != NULL) {
    assert(bt->txn_commit(txn_ptr) == BT_SUCCESS);
  }

  gettimeofday(&t2, NULL);

  if (stats) {
    cout << "OPT SP :: ";
    display_stats(t1, t2, conf.num_txns);
  }
}

