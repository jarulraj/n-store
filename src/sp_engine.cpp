// SP

#include "sp_engine.h"

using namespace std;

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

  // Check if key exists
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

    db->dirs->insert(key, after_rec);
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

  // Check if key does not exist
  if (indices->at(0)->map->contains(key) == 0) {
    return;
  }

  record* before_rec = indices->at(0)->map->at(key);

  // Remove entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    table_index_id_str = std::to_string(index_itr);
    key_str = get_data(rec_ptr, indices->at(index_itr)->sptr);
    key = hash_fn(table_id_str + table_index_id_str + key_str);

    db->dirs->remove(key);
  }

}

void sp_engine::update(const statement& st) {
  LOG_INFO("Update");
  record* rec_ptr = st.rec_ptr;
  plist<table_index*>* indices = db->tables->at(st.table_id)->indices;

  std::string key_str = get_data(rec_ptr, indices->at(0)->sptr);
  unsigned long key = hash_fn(key_str);

  record* before_rec = indices->at(0)->map->at(key);

  // Check if key does not exist
  if (before_rec == 0)
    return;

  void *before_field, *after_field;
  int num_fields = st.field_ids.size();

  for (int field_itr : st.field_ids) {
    // Pointer field
    if (rec_ptr->sptr->columns[field_itr].inlined == 0) {
      before_field = before_rec->get_pointer(field_itr);
      after_field = rec_ptr->get_pointer(field_itr);
    }
    // Data field
    else {
      before_field = before_rec->get_pointer(field_itr);
      std::string before_data = before_rec->get_data(field_itr);
    }
  }

  for (int field_itr : st.field_ids) {
    // Update existing record
    before_rec->set_data(field_itr, rec_ptr);
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

  timespec time1, time2;
  clock_gettime(CLOCK_REALTIME, &time1);

  for (const transaction& txn : load.txns)
    execute(txn);

  clock_gettime(CLOCK_REALTIME, &time2);

  if (stats)
    display_stats(time1, time2, load.txns.size());

}

