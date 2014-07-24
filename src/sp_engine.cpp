// SP - based on FS

#include "sp_engine.h"

using namespace std;

void sp_engine::group_commit() {

  while (ready) {

    if (txn_ptr != NULL) {
      wrlock(&cow_pbtree_rwlock);
      assert(bt->txn_commit(txn_ptr) == BT_SUCCESS);

      txn_ptr = bt->txn_begin(0);
      assert(txn_ptr);
      unlock(&cow_pbtree_rwlock);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(conf.gc_interval));
  }
}

sp_engine::sp_engine(const config& _conf)
    : conf(_conf),
      db(conf.db),
      bt(NULL),
      txn_ptr(NULL) {

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
  struct cow_btval key, val;

  //cout << "Select :: Key : -" << st.key << "-  -" << st.table_id << "-" << st.table_index_id << "-" << endl;
  unsigned long key_id = hasher(hash_fn(st.key), st.table_id,
                                st.table_index_id);
  string key_str = std::to_string(key_id);
  key.data = (void*) key_str.c_str();
  key.size = key_str.size();
  std::string tuple;
  //cout << "Select :: Key : -" << st.key << "-  -" << key_str << "-" << endl;

  // Read from latest clean version
  if (bt->at(txn_ptr, &key, &val) != BT_FAIL) {
    tuple = std::string((char*) val.data);
    tuple = deserialize_to_string(tuple, st.projection, false);
    LOG_INFO("val : %s", tuple.c_str());
  }

  //cout<<"val : "<<tuple<<endl;
  delete rec_ptr;

  return tuple;
}

void sp_engine::insert(const statement& st) {
  LOG_INFO("Insert");
  record* after_rec = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;
  struct cow_btval key, val;

  std::string key_str = get_data(after_rec, indices->at(0)->sptr);
  //cout << "Insert :: Key : -" << key_str << "-  -" << st.table_id << "-0-"<< endl;
  unsigned long key_id = hasher(hash_fn(key_str), st.table_id, 0);
  key_str = std::to_string(key_id);
  //cout << "Insert :: Key :: -" << key_str << "- " << endl;

  key.data = (void*) key_str.c_str();
  key.size = key_str.size();

  // Check if key exists in current version
  if (bt->at(txn_ptr, &key, &val) != BT_FAIL) {
    delete after_rec;
    return;
  }

  std::string after_tuple = serialize(after_rec, after_rec->sptr, false);

  // Add entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = get_data(after_rec, indices->at(index_itr)->sptr);
    key_id = hasher(hash_fn(key_str), st.table_id, index_itr);
    key_str = std::to_string(key_id);

    key.data = (void*) key_str.c_str();
    key.size = key_str.size();
    val.data = (void*) after_tuple.c_str();

    //printf("Val :: %s \n", (char*) val.data);

    val.size = after_tuple.size() + 1;
    bt->insert(txn_ptr, &key, &val);
  }

  delete after_rec;
}

void sp_engine::remove(const statement& st) {
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
    delete rec_ptr;
    return;
  }

  // Remove entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = get_data(rec_ptr, indices->at(index_itr)->sptr);
    key_id = hasher(hash_fn(st.key), st.table_id, index_itr);
    key_str = std::to_string(key_id);

    key.data = (void*) key_str.c_str();
    key.size = key_str.size();

    bt->remove(txn_ptr, &key, NULL);
  }

  delete rec_ptr;
}

void sp_engine::update(const statement& st) {
  LOG_INFO("Update");
  record* rec_ptr = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;
  struct cow_btval key, val, update_val;

  std::string key_str = get_data(rec_ptr, indices->at(0)->sptr);
  unsigned long key_id = hasher(hash_fn(key_str), st.table_id, 0);
  //cout << "Update :: Key : -" << key_str << "-  -" << st.table_id << "-0-" << endl;

  key_str = std::to_string(key_id);

  key.data = (void*) key_str.c_str();
  key.size = key_str.size();
  //cout << "Update :: Key : -" << key_str << endl;

  // Check if key does not exist in current version
  if (bt->at(txn_ptr, &key, &val) == BT_FAIL) {
    delete rec_ptr;
    return;
  }

  // Read from current version
  std::string before_tuple, after_tuple;

  before_tuple = std::string((char*) val.data);
  record* before_rec = deserialize_to_record(before_tuple, tab->sptr, false);

  void *before_field, *after_field;
  int num_fields = st.field_ids.size();

  // Update record
  for (int field_itr : st.field_ids) {
    before_rec->set_data(field_itr, rec_ptr);
  }

  after_tuple = serialize(before_rec, tab->sptr, false);

  // Update entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = get_data(before_rec, indices->at(index_itr)->sptr);
    key_id = hasher(hash_fn(key_str), st.table_id, index_itr);
    key_str = std::to_string(key_id);

    key.data = (void*) key_str.c_str();
    key.size = key_str.size();

    update_val.data = (void*) after_tuple.c_str();
    //printf("Update_val :: %s \n", (char*) update_val.data);

    update_val.size = after_tuple.size() + 1;
    bt->insert(txn_ptr, &key, &update_val);
  }

  delete rec_ptr;
  delete before_rec;
}

// RUNNER + LOADER

void sp_engine::execute(const transaction& txn) {

  rdlock(&cow_pbtree_rwlock);

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

  unlock(&cow_pbtree_rwlock);

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

  bt = db->dirs->t_ptr;
  txn_ptr = bt->txn_begin(0);
  assert(txn_ptr);
  txn_counter = 0;
  unsigned int num_txns = load.txns.size();
  unsigned int period = ((num_txns > 10) ? (num_txns / 10) : 1);

  std::thread gc(&sp_engine::group_commit, this);
  ready = true;

  struct timeval t1, t2;
  gettimeofday(&t1, NULL);

  for (const transaction& txn : load.txns) {
    execute(txn);

    if (++txn_counter % period == 0) {
      printf("Finished :: %.2lf %% \r",
             ((double) (txn_counter * 100) / num_txns));
      fflush(stdout);
    }
  }
  printf("\n");

  ready = false;
  gc.join();

  //if (txn_ptr != NULL) {
  assert(bt->txn_commit(txn_ptr) == BT_SUCCESS);
  //}
  txn_ptr = NULL;

  gettimeofday(&t2, NULL);

  if (stats) {
    cout << "SP :: ";
    display_stats(t1, t2, conf.num_txns);
  }
}

