// OPT SP - based on PM

#include "opt_sp_engine.h"

using namespace std;

void opt_sp_engine::group_commit() {

  while (ready) {

    if (!read_only && txn_ptr != NULL) {
      wrlock(&gc_rwlock);

      if (tid == 0) {
        wrlock(db_dirs_rwlock);

        assert(bt->txn_commit(txn_ptr) == BT_SUCCESS);
        txn_ptr = bt->txn_begin(0);
        assert(txn_ptr);

        // Clear commit_free list
        for (void* ptr : db->commit_free_list) {
          //FIXME pmemalloc_free(ptr);
        }
        db->commit_free_list.clear();

        unlock(db_dirs_rwlock);
      }

      unlock(&gc_rwlock);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(conf.gc_interval));
  }
}

opt_sp_engine::opt_sp_engine(const config& _conf, bool _read_only,
                             unsigned int _tid)
    : conf(_conf),
      db(conf.db),
      bt(NULL),
      txn_ptr(NULL),
      tid(_tid) {

  etype = engine_type::OPT_SP;
  read_only = _read_only;
  db_dirs_rwlock = &db->engine_rwlock;

  bt = db->dirs->t_ptr;
  if (tid == 0) {
    wrlock(db_dirs_rwlock);
    txn_ptr = bt->txn_begin(read_only);
    assert(txn_ptr);
    unlock(db_dirs_rwlock);
  }

  // Commit only if needed
  if (!read_only) {
    gc = std::thread(&opt_sp_engine::group_commit, this);
    ready = true;
  }

}

opt_sp_engine::~opt_sp_engine() {

  if (!read_only) {
    ready = false;
    gc.join();
  }

  if (!read_only && txn_ptr != NULL) {
    assert(bt->txn_commit(txn_ptr) == BT_SUCCESS);
  }
  txn_ptr = NULL;

}

std::string opt_sp_engine::select(const statement& st) {
  LOG_INFO("Select");
  record* rec_ptr = st.rec_ptr;
  record* select_ptr;
  struct cow_btval key, val;
  table* tab = db->tables->at(st.table_id);
  table_index* table_index = tab->indices->at(st.table_index_id);
  std::string key_str = serialize(rec_ptr, table_index->sptr);

  unsigned long key_id = hasher(hash_fn(key_str), st.table_id,
                                st.table_index_id);
  std::string comp_key_str = std::to_string(key_id);
  key.data = (void*) comp_key_str.c_str();
  key.size = comp_key_str.size();
  std::string value;

  // Read from latest clean version
  rdlock(db_dirs_rwlock);
  if (bt->at(txn_ptr, &key, &val) != BT_FAIL) {
    memcpy(&select_ptr, val.data, sizeof(record*));
    //printf("select_ptr :: --%p-- \n", select_ptr);
    value = serialize(select_ptr, st.projection);
  }
  unlock(db_dirs_rwlock);

  LOG_INFO("val : %s", value.c_str());
  //cout<<"val : " <<value<<endl;

  delete rec_ptr;
  return value;
}

int opt_sp_engine::insert(const statement& st) {
  LOG_INFO("Insert");
  record* after_rec = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;
  struct cow_btval key, val;
  val.data = new char[sizeof(record*) + 1];

  std::string key_str = serialize(after_rec, indices->at(0)->sptr);
  unsigned long key_id = hasher(hash_fn(key_str), st.table_id, 0);
  key_str = std::to_string(key_id);

  key.data = (void*) key_str.c_str();
  key.size = key_str.size();

  // Check if key exists in current version
  rdlock(db_dirs_rwlock);
  if (bt->at(txn_ptr, &key, &val) != BT_FAIL) {
    delete after_rec;
    unlock(db_dirs_rwlock);
    return EXIT_SUCCESS;
  }
  unlock(db_dirs_rwlock);

  // Activate new record
  pmemalloc_activate(after_rec);
  after_rec->persist_data();

  // Add entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = serialize(after_rec, indices->at(index_itr)->sptr);
    key_id = hasher(hash_fn(key_str), st.table_id, index_itr);
    key_str = std::to_string(key_id);

    key.data = (void*) key_str.c_str();
    key.size = key_str.size();
    memcpy(val.data, &after_rec, sizeof(record*));
    val.size = sizeof(record*);

    wrlock(db_dirs_rwlock);
    bt->insert(txn_ptr, &key, &val);
    unlock(db_dirs_rwlock);
  }

  delete ((char*) val.data);
  return EXIT_SUCCESS;
}

int opt_sp_engine::remove(const statement& st) {
  LOG_INFO("Remove");
  record* rec_ptr = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;
  struct cow_btval key, val;

  std::string key_str = serialize(rec_ptr, indices->at(0)->sptr);
  unsigned long key_id = hasher(hash_fn(key_str), st.table_id, 0);
  key_str = std::to_string(key_id);

  key.data = (void*) key_str.c_str();
  key.size = key_str.size();

  // Check if key does not exist
  rdlock(db_dirs_rwlock);
  if (bt->at(txn_ptr, &key, &val) == BT_FAIL) {
    delete rec_ptr;
    unlock(db_dirs_rwlock);
    return EXIT_SUCCESS;
  }
  unlock(db_dirs_rwlock);

  // Free record
  record* before_rec;
  memcpy(&before_rec, val.data, sizeof(record*));

  // Remove entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = serialize(rec_ptr, indices->at(index_itr)->sptr);
    key_id = hasher(hash_fn(key_str), st.table_id, index_itr);
    key_str = std::to_string(key_id);

    key.data = (void*) key_str.c_str();
    key.size = key_str.size();

    wrlock(db_dirs_rwlock);
    bt->remove(txn_ptr, &key, NULL);
    unlock(db_dirs_rwlock);
  }

  delete rec_ptr;
  wrlock(db_dirs_rwlock);
  db->commit_free_list.insert(before_rec);
  for (int field_itr : st.field_ids) {
    if (rec_ptr->sptr->columns[field_itr].inlined == 0) {
      void* before_field = before_rec->get_pointer(field_itr);
      db->commit_free_list.insert(before_field);
    }
  }
  unlock(db_dirs_rwlock);
  return EXIT_SUCCESS;
}

int opt_sp_engine::update(const statement& st) {
  LOG_INFO("Update");
  record* rec_ptr = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;
  struct cow_btval key, val, update_val;

  std::string key_str = serialize(rec_ptr, indices->at(0)->sptr);
  unsigned long key_id = hasher(hash_fn(key_str), st.table_id, 0);
  key_str = std::to_string(key_id);
  key.data = (void*) key_str.c_str();
  key.size = key_str.size();

  // Check if key does not exist in current version
  wrlock(db_dirs_rwlock);
  if (bt->at(txn_ptr, &key, &val) == BT_FAIL) {
    delete rec_ptr;
    unlock(db_dirs_rwlock);
    return EXIT_SUCCESS;
  }

  // Read from current version
  record* before_rec;
  memcpy(&before_rec, val.data, sizeof(record*));

  void *before_field, *after_field;
  assert(before_rec->data != NULL);

  record* after_rec = new record(before_rec->sptr);
  memcpy(after_rec->data, before_rec->data, before_rec->data_len);

  // Update record
  for (int field_itr : st.field_ids) {
    if (rec_ptr->sptr->columns[field_itr].inlined == 0) {
      before_field = before_rec->get_pointer(field_itr);
      after_field = rec_ptr->get_pointer(field_itr);
      pmemalloc_activate(after_field);
      //FIXME delete ((char*) before_field);
    }

    after_rec->set_data(field_itr, rec_ptr);
  }

  // Activate new record
  pmemalloc_activate(after_rec);
  after_rec->persist_data();

  update_val.data = new char[sizeof(record*) + 1];
  memcpy(update_val.data, &after_rec, sizeof(record*));
  update_val.size = sizeof(record*);

  // Update entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = serialize(after_rec, indices->at(index_itr)->sptr);
    key_id = hasher(hash_fn(key_str), st.table_id, index_itr);
    key_str = std::to_string(key_id);

    key.data = (void*) key_str.c_str();
    key.size = key_str.size();

    bt->remove(txn_ptr, &key, NULL);
    bt->insert(txn_ptr, &key, &update_val);
  }

  //printf("before_rec :: record :: %p \n", before_rec);
  //printf("rec_ptr :: record :: %p \n", rec_ptr);

  delete rec_ptr;
  db->commit_free_list.insert(before_rec);
  delete ((char*) update_val.data);
  unlock(db_dirs_rwlock);

  return EXIT_SUCCESS;
}

void opt_sp_engine::load(const statement& st) {
  //LOG_INFO("Load");
  record* after_rec = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;
  struct cow_btval key, val;
  val.data = new char[sizeof(record*) + 1];

  std::string key_str = serialize(after_rec, indices->at(0)->sptr);
  unsigned long key_id = hasher(hash_fn(key_str), st.table_id, 0);
  key_str = std::to_string(key_id);

  // Activate new record
  pmemalloc_activate(after_rec);
  after_rec->persist_data();

  // Add entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = serialize(after_rec, indices->at(index_itr)->sptr);
    key_id = hasher(hash_fn(key_str), st.table_id, index_itr);
    key_str = std::to_string(key_id);

    key.data = (void*) key_str.c_str();
    key.size = key_str.size();
    memcpy(val.data, &after_rec, sizeof(record*));
    val.size = sizeof(record*);

    bt->insert(txn_ptr, &key, &val);
  }

  delete ((char*) val.data);
}

void opt_sp_engine::txn_begin() {
  if (!read_only) {
    wrlock(&gc_rwlock);
  }
}

void opt_sp_engine::txn_end(bool commit) {
  if (!read_only) {
    unlock(&gc_rwlock);
  }
}

void opt_sp_engine::recovery() {

  cout << "OPT_SP :: Recovery duration (ms) : " << 0.0 << endl;

}

