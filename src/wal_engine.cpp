// WRITE-AHEAD LOGGING

#include <cstring>
#include <cstdlib>
#include <fstream>

#include "wal_engine.h"

using namespace std;

void wal_engine::group_commit() {
  std::unique_lock<std::mutex> lk(gc_mutex);
  cv.wait(lk, [&] {return ready;});

  while (ready) {
    if (conf.verbose)
      std::cout << "Syncing log !" << endl;

    // sync
    undo_log.write();

    std::this_thread::sleep_for(std::chrono::milliseconds(conf.gc_interval));
  }
}

int wal_engine::update(const statement* st) {

  record* rec_ptr = st->rec_ptr;
  unsigned int table_id = st->table_id;

  const vector<table_index>& indices = load.tables[table_id].indices;
  vector<table_index>::const_iterator index_itr;
  int field_id = st->field_id;

  for (index_itr = indices.begin(); index_itr != indices.end(); index_itr++) {

    std::string key = get_data(rec_ptr, (*index_itr).key);

    if ((*index_itr).index.count(key) == 0) {
      return -1;
    }

    record* before_rec = (*index_itr).index.at(key);

    field* before_field = before_rec->data.at(field_id);
    field* after_field = st->field_ptr;

    before_rec->data.at(field_id) = after_field;

    // Add log entry
    vector<field*> before_image = { before_field };
    vector<field*> after_image = { after_field };

    entry e(st, field_id, before_image, after_image);
    undo_log.push(e);
  }

  return 0;
}

std::string wal_engine::select(const statement* st) {
  record* rec_ptr = st->rec_ptr;
  unsigned int table_id = st->table_id;

  unsigned int table_index_id = st->table_index_id;
  table_index& table_index = load.tables[table_id].indices[table_index_id];
  vector<bool> projection = st->projection;

  std::string key = get_data(rec_ptr, table_index.key);
  std::string val;

  if (table_index.index.count(key) == 0) {
    return NULL;
  }

  record* r = table_index.index[key];
  val = get_data(r, projection);

  cout << "val :" << val << endl;

  return val;
}

int wal_engine::insert(const statement* st) {
  record* rec_ptr = st->rec_ptr;
  unsigned int table_id = st->table_id;

  vector<table_index>& indices = load.tables[table_id].indices;
  vector<table_index>::iterator index_itr;
  int field_id = st->field_id;

  for (index_itr = indices.begin(); index_itr != indices.end(); index_itr++) {

    std::string key = get_data(rec_ptr, (*index_itr).key);

    // check if key already exists
    if ((*index_itr).index.count(key) != 0) {
      return -1;
    }

    record* after_rec = st->rec_ptr;

    (*index_itr).index[key] = after_rec;

    vector<field*> before_image;

    // Add log entry
    entry e(st, field_id, before_image, after_rec->data);
    undo_log.push(e);
  }

  return 0;
}

// RUNNER + LOADER

void wal_engine::handle_message(const message& msg) {
  const statement* st_ptr = msg.st_ptr;

  unsigned int s_id = msg.statement_id;

  if (st_ptr->op_type == operation_type::Insert) {
    insert (st_ptr);
  } else if (st_ptr->op_type == operation_type::Select) {
    select (st_ptr);
  } else if (st_ptr->op_type == operation_type::Update) {
    update (st_ptr);
  }

}

void wal_engine::runner() {
  int consumer_count;
  message msg;

  undo_log.set_path(conf.fs_path + "./log_" + std::to_string(partition_id),
                    "w");

  // Logger start
  std::thread gc(&wal_engine::group_commit, this);
  {
    std::lock_guard<std::mutex> lk(gc_mutex);
    ready = true;
  }
  cv.notify_one();

  while (!done) {
    if (!msg_queue.empty()) {
      msg = msg_queue.front();
      msg_queue.pop();

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
