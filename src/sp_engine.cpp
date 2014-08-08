// SP - based on FS

#include "sp_engine.h"

using namespace std;

void sp_engine::group_commit() {

  while (ready) {

    if (!read_only && txn_ptr != NULL) {
      //cout<<"gc lock"<<endl;
      wrlock(&gc_rwlock);

      if (tid == 0) {
        mt_wrlock(db_dirs_rwlock_ptr);
        assert(bt->txn_commit(txn_ptr) == BT_SUCCESS);
        txn_ptr = bt->txn_begin(0);
        mt_unlock(db_dirs_rwlock_ptr);
        assert(txn_ptr);
      }

      //cout<<"gc unlock"<<endl;
      unlock(&gc_rwlock);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(conf.gc_interval));
  }
}

sp_engine::sp_engine(const config& _conf, bool _read_only, unsigned int _tid)
    : conf(_conf),
      db(conf.db),
      bt(NULL),
      txn_ptr(NULL),
      tid(_tid) {

  etype = engine_type::SP;
  read_only = _read_only;
  db_dirs_rwlock_ptr = &db->db_dirs_rwlock_ptr;
  if (conf.num_executors > 0)
    multithreaded = true;

  bt = db->dirs->t_ptr;
  if (tid == 0) {
    mt_wrlock(db_dirs_rwlock_ptr);
    txn_ptr = bt->txn_begin(read_only);
    assert(txn_ptr);
    mt_unlock(db_dirs_rwlock_ptr);
  }

  // Commit only if needed
  if (!read_only) {
    gc = std::thread(&sp_engine::group_commit, this);
    ready = true;
  }

}

sp_engine::~sp_engine() {

  if (!read_only) {
    ready = false;
    gc.join();
  }

  if (!read_only && txn_ptr != NULL && tid == 0) {
    assert(bt->txn_commit(txn_ptr) == BT_SUCCESS);
  }

  txn_ptr = NULL;

}

std::string sp_engine::select(const statement& st) {
  LOG_INFO("Select");
  record* rec_ptr = st.rec_ptr;
  struct cow_btval key, val;
  table* tab = db->tables->at(st.table_id);
  table_index* table_index = tab->indices->at(st.table_index_id);
  std::string key_str = serialize(rec_ptr, table_index->sptr);

  //cout << "Select :: Key : -" << st.key << "-  -" << st.table_id << "-" << st.table_index_id << "-" << endl;
  unsigned long key_id = hasher(hash_fn(key_str), st.table_id,
                                st.table_index_id);
  std::string comp_key_str = std::to_string(key_id);
  key.data = (void*) comp_key_str.c_str();
  key.size = comp_key_str.size();
  std::string tuple;
  //cout << "Select :: Key : -" << st.key << "-  -" << key_str << "-" << endl;

  // Read from latest clean version
  mt_rdlock(db_dirs_rwlock_ptr);
  if (bt->at(txn_ptr, &key, &val) != BT_FAIL) {
    tuple = std::string((char*) val.data);
    tuple = deserialize_to_string(tuple, st.projection);
    LOG_INFO("val : %s", tuple.c_str());
  }
  mt_unlock(db_dirs_rwlock_ptr);

  //cout<<"val : "<<tuple<<endl;
  delete rec_ptr;
  return tuple;
}

int sp_engine::insert(const statement& st) {
  LOG_INFO("Insert");
  record* after_rec = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;
  struct cow_btval key, val;

  std::string key_str = serialize(after_rec, indices->at(0)->sptr);
  //cout << "Insert :: Key : -" << key_str << "-  -" << st.table_id << "-0-"<< endl;
  unsigned long key_id = hasher(hash_fn(key_str), st.table_id, 0);
  key_str = std::to_string(key_id);
  //cout << "Insert :: Key :: -" << key_str << "- " << endl;

  key.data = (void*) key_str.c_str();
  key.size = key_str.size();

  // Check if key exists in current version
  mt_rdlock(db_dirs_rwlock_ptr);
  if (bt->at(txn_ptr, &key, &val) != BT_FAIL) {
    delete after_rec;
    mt_unlock(db_dirs_rwlock_ptr);
    return EXIT_SUCCESS;
  }
  mt_unlock(db_dirs_rwlock_ptr);

  std::string after_tuple = serialize(after_rec, after_rec->sptr);

  // Add entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = serialize(after_rec, indices->at(index_itr)->sptr);
    key_id = hasher(hash_fn(key_str), st.table_id, index_itr);
    key_str = std::to_string(key_id);

    key.data = (void*) key_str.c_str();
    key.size = key_str.size();
    val.data = (void*) after_tuple.c_str();

    //printf("Val :: %s \n", (char*) val.data);

    val.size = after_tuple.size() + 1;

    mt_wrlock(db_dirs_rwlock_ptr);
    bt->insert(txn_ptr, &key, &val);
    mt_unlock(db_dirs_rwlock_ptr);
  }

  delete after_rec;
  return EXIT_SUCCESS;
}

int sp_engine::remove(const statement& st) {
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
  mt_rdlock(db_dirs_rwlock_ptr);
  if (bt->at(txn_ptr, &key, &val) == BT_FAIL) {
    delete rec_ptr;
    mt_unlock(db_dirs_rwlock_ptr);
    return EXIT_SUCCESS;
  }
  mt_unlock(db_dirs_rwlock_ptr);

  // Remove entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = serialize(rec_ptr, indices->at(index_itr)->sptr);
    key_id = hasher(hash_fn(key_str), st.table_id, index_itr);
    key_str = std::to_string(key_id);

    key.data = (void*) key_str.c_str();
    key.size = key_str.size();

    mt_wrlock(db_dirs_rwlock_ptr);
    bt->remove(txn_ptr, &key, NULL);
    mt_unlock(db_dirs_rwlock_ptr);
  }

  delete rec_ptr;
  return EXIT_SUCCESS;
}

int sp_engine::update(const statement& st) {
  LOG_INFO("Update");
  record* rec_ptr = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;
  struct cow_btval key, val, update_val;

  std::string key_str = serialize(rec_ptr, indices->at(0)->sptr);
  unsigned long key_id = hasher(hash_fn(key_str), st.table_id, 0);
  //cout << "Update :: Key : -" << key_str << "-  -" << st.table_id << "-0-" << endl;

  key_str = std::to_string(key_id);

  key.data = (void*) key_str.c_str();
  key.size = key_str.size();
  //cout << "Update :: Key : -" << key_str << endl;

  // Check if key does not exist in current version
  mt_rdlock(db_dirs_rwlock_ptr);
  if (bt->at(txn_ptr, &key, &val) == BT_FAIL) {
    delete rec_ptr;
    mt_unlock(db_dirs_rwlock_ptr);
    return EXIT_SUCCESS;
  }
  mt_unlock(db_dirs_rwlock_ptr);

  // Read from current version
  std::string before_tuple, after_tuple;

  before_tuple = std::string((char*) val.data);
  record* before_rec = deserialize(before_tuple, tab->sptr);

  void *before_field, *after_field;
  int num_fields = st.field_ids.size();

  // Update record
  for (int field_itr : st.field_ids) {
    before_rec->set_data(field_itr, rec_ptr);
  }

  after_tuple = serialize(before_rec, tab->sptr);

  // Update entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = serialize(before_rec, indices->at(index_itr)->sptr);
    key_id = hasher(hash_fn(key_str), st.table_id, index_itr);
    key_str = std::to_string(key_id);

    key.data = (void*) key_str.c_str();
    key.size = key_str.size();

    update_val.data = (void*) after_tuple.c_str();
    //printf("Update_val :: %s \n", (char*) update_val.data);

    update_val.size = after_tuple.size() + 1;

    mt_wrlock(db_dirs_rwlock_ptr);
    bt->insert(txn_ptr, &key, &update_val);
    mt_unlock(db_dirs_rwlock_ptr);
  }

  delete rec_ptr;
  delete before_rec;

  return EXIT_SUCCESS;
}

void sp_engine::load(const statement& st) {
  LOG_INFO("Load");
  record* after_rec = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;
  struct cow_btval key, val;

  std::string key_str = serialize(after_rec, indices->at(0)->sptr);
  unsigned long key_id = hasher(hash_fn(key_str), st.table_id, 0);
  key_str = std::to_string(key_id);

  std::string after_tuple = serialize(after_rec, after_rec->sptr);

  // Add entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = serialize(after_rec, indices->at(index_itr)->sptr);
    key_id = hasher(hash_fn(key_str), st.table_id, index_itr);
    key_str = std::to_string(key_id);

    key.data = (void*) key_str.c_str();
    key.size = key_str.size();
    val.data = (void*) after_tuple.c_str();

    val.size = after_tuple.size() + 1;
    bt->insert(txn_ptr, &key, &val);
  }

  delete after_rec;
}

void sp_engine::txn_begin() {
  if (!read_only) {
    //cout<<"lock"<<endl;
    wrlock(&gc_rwlock);
  }
}

void sp_engine::txn_end(bool commit) {
  if (!read_only) {
    //cout<<"unlock"<<endl;
    unlock(&gc_rwlock);
  }
}

void sp_engine::recovery() {

  cout << "SP :: Recovery duration (ms) : " << 0.0 << endl;

}
