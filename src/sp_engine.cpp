// SP - based on FS

#include "sp_engine.h"

using namespace std;

sp_engine::sp_engine(const config& _conf, database* _db, bool _read_only,
                     unsigned int _tid)
    : conf(_conf),
      db(_db),
      bt(NULL),
      txn_ptr(NULL),
      tid(_tid) {

  etype = engine_type::SP;
  read_only = _read_only;

  bt = db->dirs->t_ptr;
  txn_ptr = bt->txn_begin(read_only);
  assert(txn_ptr);

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

  if (!read_only && txn_ptr != NULL) {
    assert(bt->txn_commit(txn_ptr) == BT_SUCCESS);
  }

  txn_ptr = NULL;

  if(conf.storage_stats)
    bt->compact();

}

std::string sp_engine::select(const statement& st) {
  LOG_INFO("Select");
  record* rec_ptr = st.rec_ptr;
  struct cow_btval key, val;
  table* tab = db->tables->at(st.table_id);
  table_index* table_index = tab->indices->at(st.table_index_id);
  std::string key_str = sr.serialize(rec_ptr, table_index->sptr);

  unsigned long key_id = hasher(hash_fn(key_str), st.table_id,
                                st.table_index_id);
  std::string comp_key_str = std::to_string(key_id);
  key.data = (void*) comp_key_str.c_str();
  key.size = comp_key_str.size();
  std::string tuple;

  // Read from latest clean version
  if (bt->at(txn_ptr, &key, &val) != BT_FAIL) {
    tuple = std::string((char*) val.data);
    tuple = sr.project(tuple, st.projection);
    LOG_INFO("val : --%s--", tuple.c_str());
  }

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

  std::string key_str = sr.serialize(after_rec, indices->at(0)->sptr);
  unsigned long key_id = hasher(hash_fn(key_str), st.table_id, 0);
  key_str = std::to_string(key_id);

  key.data = (void*) key_str.c_str();
  key.size = key_str.size();

  // Check if key present in current version
  if (bt->at(txn_ptr, &key, &val) != BT_FAIL) {
    after_rec->clear_data();
    delete after_rec;
    return EXIT_SUCCESS;
  }

  std::string after_tuple = sr.serialize(after_rec, after_rec->sptr);
  val.data = (void*) after_tuple.c_str();
  val.size = after_tuple.size();

  // Add entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = sr.serialize(after_rec, indices->at(index_itr)->sptr);
    key_id = hasher(hash_fn(key_str), st.table_id, index_itr);
    key_str = std::to_string(key_id);

    key.data = (void*) key_str.c_str();
    key.size = key_str.size();

    bt->insert(txn_ptr, &key, &val);
  }

  after_rec->clear_data();
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

  std::string key_str = sr.serialize(rec_ptr, indices->at(0)->sptr);
  unsigned long key_id = hasher(hash_fn(key_str), st.table_id, 0);
  key_str = std::to_string(key_id);
  key.data = (void*) key_str.c_str();
  key.size = key_str.size();

  // Check if key does not exist
  if (bt->at(txn_ptr, &key, &val) == BT_FAIL) {
    delete rec_ptr;
    return EXIT_SUCCESS;
  }

  // Remove entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = sr.serialize(rec_ptr, indices->at(index_itr)->sptr);
    key_id = hasher(hash_fn(key_str), st.table_id, index_itr);
    key_str = std::to_string(key_id);

    key.data = (void*) key_str.c_str();
    key.size = key_str.size();

    bt->remove(txn_ptr, &key, NULL);
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
  std::string key_str = sr.serialize(rec_ptr, indices->at(0)->sptr);

  unsigned long key_id = hasher(hash_fn(key_str), st.table_id, 0);
  key_str = std::to_string(key_id);
  key.data = (void*) key_str.c_str();
  key.size = key_str.size();

  // Check if key does not exist in current version
  if (bt->at(txn_ptr, &key, &val) == BT_FAIL) {
    rec_ptr->clear_data();
    delete rec_ptr;
    return EXIT_SUCCESS;
  }

  // Read from current version
  std::string before_tuple, after_tuple;

  before_tuple = std::string((char*) val.data);
  record* before_rec = sr.deserialize(before_tuple, tab->sptr);

  // Update record
  for (int field_itr : st.field_ids) {
    before_rec->set_data(field_itr, rec_ptr);
  }

  after_tuple = sr.serialize(before_rec, tab->sptr);

  update_val.data = (void*) after_tuple.c_str();
  update_val.size = after_tuple.size();

  // Update entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = sr.serialize(before_rec, indices->at(index_itr)->sptr);
    key_id = hasher(hash_fn(key_str), st.table_id, index_itr);
    key_str = std::to_string(key_id);

    key.data = (void*) key_str.c_str();
    key.size = key_str.size();

    //bt->remove(txn_ptr, &key, NULL);
    bt->insert(txn_ptr, &key, &update_val);
  }

  delete rec_ptr;
  before_rec->clear_data();
  delete before_rec;

  return EXIT_SUCCESS;
}

void sp_engine::load(const statement& st) {
  //LOG_INFO("Load");
  record* after_rec = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;
  struct cow_btval key, val;

  std::string key_str = sr.serialize(after_rec, indices->at(0)->sptr);
  unsigned long key_id = hasher(hash_fn(key_str), st.table_id, 0);
  key_str = std::to_string(key_id);

  std::string after_tuple = sr.serialize(after_rec, after_rec->sptr);

  val.data = (void*) after_tuple.c_str();
  val.size = after_tuple.size();

  // Add entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = sr.serialize(after_rec, indices->at(index_itr)->sptr);
    key_id = hasher(hash_fn(key_str), st.table_id, index_itr);
    key_str = std::to_string(key_id);

    key.data = (void*) key_str.c_str();
    key.size = key_str.size();

    bt->insert(txn_ptr, &key, &val);
  }

  after_rec->clear_data();
  delete after_rec;
}

void sp_engine::group_commit() {

  while (ready) {

    if (!read_only && txn_ptr != NULL) {
      wrlock(&gc_rwlock);
      assert(bt->txn_commit(txn_ptr) == BT_SUCCESS);
      txn_ptr = bt->txn_begin(0);
      assert(txn_ptr);
      unlock(&gc_rwlock);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(conf.gc_interval));
  }
}

void sp_engine::txn_begin() {
  if (!read_only) {
    wrlock(&gc_rwlock);
  }
}

void sp_engine::txn_end(__attribute__((unused)) bool commit) {
  if (!read_only) {
    unlock(&gc_rwlock);
  }
}

void sp_engine::recovery() {

  cout << "SP :: Recovery duration (ms) : " << 0.0 << endl;

}
