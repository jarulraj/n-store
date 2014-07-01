// WRITE-AHEAD LOGGING

#include <cstring>
#include <cstdlib>
#include <fstream>

#include "wal_engine.h"

using namespace std;

void wal_engine::group_commit() {
  while (ready) {
    //std::cout << "Syncing log !" << endl;

    // sync
    undo_log.write();

    std::this_thread::sleep_for(std::chrono::milliseconds(conf.gc_interval));
  }
}

std::string wal_engine::select(const statement& st) {
  record* rec_ptr = st.rec_ptr;
  unsigned int table_id = st.table_id;
  std::string val;

  /*
  unsigned int table_index_id = st.table_index_id;
  table_index* table_index = db->tables[table_id]->indices[table_index_id];
  bool* projection = st.projection;

  std::string key = get_data(rec_ptr, table_index->key);

  if (table_index->index.count(key) == 0) {
    return NULL;
  }

  record* r = table_index->index[key];
  val = get_data(r, projection);

  cout << "val :" << val << endl;
  */

  return val;
}

int wal_engine::insert(const statement& st) {
  record* rec_ptr = st.rec_ptr;
  unsigned int table_id = st.table_id;

  table* tab = db->tables->at(table_id);
  plist<table_index*>* indices = tab->indices;
  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;
  int field_id = st.field_id;
  std::hash<std::string> hash_fn;

  for (index_itr = 0; index_itr < num_indices; index_itr++) {

    std::string key_str = get_data(rec_ptr, indices->at(index_itr)->sptr);
    cout<<"key :: "<<key_str<<endl;
    unsigned long key = hash_fn(key_str);

    // check if key already exists
    if (indices->at(index_itr)->map->contains(key) != 0) {
      return -1;
    }

    record* after_rec = st.rec_ptr;
    pmemalloc_activate(after_rec);

    indices->at(index_itr)->map->insert(key, after_rec);

    // Add log entry
    //entry e(st.transaction_id, st.op_type, st.table_id, after_rec->num_fields, after_rec->fields, -1, NULL);
    //undo_log.push(e);
  }

  return 0;
}

int wal_engine::update(const statement& st) {

  record* rec_ptr = st.rec_ptr;
  unsigned int table_id = st.table_id;

  /*
  table* tab = PMEM(db->tables)->at(table_id);
  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;
  int field_id = st.field_id;
  std::hash<std::string> hash_fn;

  for (index_itr = 0; index_itr < num_indices; index_itr++) {

    std::string key_str = get_data(rec_ptr, PMEM(PMEM(tab->indices)->at(index_itr)->key));
    unsigned long key = hash_fn(key_str);

    // check if key already exists
    if (PMEM(PMEM(tab->indices)->at(index_itr)->map)->contains(key) != 0) {
      return -1;
    }

    record* before_rec = PMEM(PMEM(tab->indices)->at(index_itr)->map)->at(key);

    field* before_field = before_rec->fields[field_id];
    field* after_field = st.field_ptr;

    before_rec->fields[field_id] = after_field;

    // Add log entry
    entry e(st.transaction_id, st.op_type, st.table_id, rec_ptr->num_fields, rec_ptr->fields, field_id, after_field);
    undo_log.push(e);
  }
  */

  return 0;
}

// RUNNER + LOADER

void wal_engine::handle_message(const message& msg) {
  const statement& st = msg.st;

  unsigned int s_id = msg.statement_id;

  if (st.op_type == operation_type::Insert) {
    insert(st);
  } else if (st.op_type == operation_type::Select) {
    select(st);
  } else if (st.op_type == operation_type::Update) {
    update(st);
  }

}

void wal_engine::runner() {
  int consumer_count;
  message msg;

  undo_log.configure(conf.fs_path + "./log_" + std::to_string(partition_id));

  // Logger start
  std::thread gc(&wal_engine::group_commit, this);
  ready = true;

  bool empty = true;
  while (!done) {

    rdlock(&msg_queue_rwlock);
    empty = msg_queue.empty();
    unlock(&msg_queue_rwlock);

    if (!empty) {
      wrlock(&msg_queue_rwlock);
      msg = msg_queue.front();
      msg_queue.pop();
      unlock(&msg_queue_rwlock);
      handle_message(msg);
    }
  }

  while (!msg_queue.empty()) {
    msg = msg_queue.front();
    msg_queue.pop();
    handle_message(msg);
  }

  // Logger end
  ready = false;
  gc.join();

  undo_log.write();
}

int wal_engine::test() {

  // Recovery
  //check();

  /*
   start = std::chrono::high_resolution_clock::now();

   recovery();

   finish = std::chrono::high_resolution_clock::now();
   elapsed_seconds = finish - start;
   std::cout << "Recovery duration: " << elapsed_seconds.count() << endl;

   check();
   */

  return 0;
}
