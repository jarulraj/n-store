// SP

#include "sp_engine.h"

using namespace std;

void sp_engine::group_commit() {

  while (ready) {
    unsigned int version;

    wrlock(&ptreap_rwlock);
    db->dirs->next_version();
    version = db->dirs->version;
    db->version = version-1;

    //db->dirs->delete_versions(version-2);
    unlock(&ptreap_rwlock);

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
  //LOG_INFO("Select");
  record* rec_ptr = st.rec_ptr;

  unsigned long key = hasher(hash_fn(st.key), st.table_id, st.table_index_id);
  std::string val;
  //cout<<"select key :: -"<<st.key<<"-  --"<<key<<endl;

  // Read from latest clean version
  rec_ptr = db->dirs->at(key, db->version);
  val = get_data(rec_ptr, st.projection);
  LOG_INFO("val : %s", val.c_str());

  return val;
}

void sp_engine::insert(const statement& st) {
  //LOG_INFO("Insert");
  record* after_rec = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;

  std::string key_str = get_data(after_rec, indices->at(0)->sptr);
  unsigned long key = hasher(hash_fn(key_str), st.table_id, 0);
  //cout<<"insert key :: -"<<key_str<<"-  --"<<key<<endl;

  // Check if key exists in current version
  if (db->dirs->at(key) != 0) {
    return;
  }

  // Activate new record
  pmemalloc_activate(after_rec);
  after_rec->persist_data();

  // Add entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = get_data(after_rec, indices->at(index_itr)->sptr);
    key = hasher(hash_fn(key_str), st.table_id, index_itr);

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

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;

  std::string key_str = get_data(rec_ptr, indices->at(0)->sptr);
  unsigned long key = hasher(hash_fn(key_str), st.table_id, 0);

  // Check if key does not exist in current version
  if (db->dirs->at(key) == 0) {
    return;
  }

  // Free record
  record* before_rec = db->dirs->at(key);
  delete before_rec;

  // Remove entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = get_data(rec_ptr, indices->at(index_itr)->sptr);
    key = hasher(hash_fn(key_str), st.table_id, index_itr);

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

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;

  std::string key_str = get_data(rec_ptr, indices->at(0)->sptr);
  unsigned long key = hasher(hash_fn(key_str), st.table_id, 0);

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
    key_str = get_data(after_rec, indices->at(index_itr)->sptr);
    key = hasher(hash_fn(key_str), st.table_id, index_itr);

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

  if(++looper%10000 == 0){
    cout<<"version ::"<<db->dirs->version <<" ";
    cout<<"looper ::"<<looper<<endl;
  }

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

  timeval t1, t2;
  gettimeofday(&t1, NULL);

  looper = 0;
  for (const transaction& txn : load.txns)
    execute(txn);

  ready = false;
  gc.join();

  gettimeofday(&t2, NULL);

  if(stats){
    cout<<"SP :: ";
    display_stats(t1, t2, conf.num_txns);
  }
}

