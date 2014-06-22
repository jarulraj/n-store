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

int wal_engine::update(statement* st) {

  record* rec_ptr = st->rec_ptr;
  table* table_ptr = st->table_ptr;
  vector<table_index*> indices = table_ptr->indices;
  vector<table_index*>::iterator index_itr;
  int field_id = st->field_id;

  for (index_itr = indices.begin(); index_itr != indices.end(); index_itr++) {

    std::string key = get_data(rec_ptr, (*index_itr)->key);

    if ((*index_itr)->index.count(key) == 0) {
      return -1;
    }

    record* before_rec = (*index_itr)->index.at(key);

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

std::string wal_engine::select(statement* st) {
  record* rec_ptr = st->rec_ptr;
  table_index* table_index_ptr = st->table_index_ptr;
  vector<bool> projection = st->projection;

  std::string key = get_data(rec_ptr, table_index_ptr->key);
  std::string val;

  if (table_index_ptr->index.count(key) == 0) {
    return NULL;
  }

  record* r = table_index_ptr->index[key];
  val = get_data(r, projection);

  cout << "val :" << val << endl;

  return val;
}

int wal_engine::insert(statement* st) {
  record* rec_ptr = st->rec_ptr;
  table* table_ptr = st->table_ptr;
  vector<table_index*> indices = table_ptr->indices;
  vector<table_index*>::iterator index_itr;
  int field_id = st->field_id;

  for (index_itr = indices.begin(); index_itr != indices.end(); index_itr++) {

    std::string key = get_data(rec_ptr, (*index_itr)->key);

    // check if key already exists
    if ((*index_itr)->index.count(key) != 0) {
      return -1;
    }

    record* after_rec = st->rec_ptr;

    (*index_itr)->index[key] = after_rec;

    vector<field*> before_image;

    // Add log entry
    entry e(st, field_id, before_image, after_rec->data);
    undo_log.push(e);
  }

  return 0;
}

// RUNNER + LOADER

void wal_engine::handle_message(const message& msg) {
  statement* s_ptr;

  unsigned int s_id = msg.statement_id;
  s_ptr = msg.st_ptr;

  if (s_ptr->op_type == operation_type::Insert) {
    insert(s_ptr);
  } else if (s_ptr->op_type == operation_type::Select) {
    select(s_ptr);
  } else if (s_ptr->op_type == operation_type::Update) {
    update(s_ptr);
  }

}

void wal_engine::runner() {
  int consumer_count;
  message msg;

  undo_log.set_path(conf.fs_path + "./log_"+std::to_string(partition_id), "w");

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

/*
 void wal_engine::check(){
 unordered_map<unsigned int, record*>::const_iterator t_itr;

 for (t_itr = table_index.begin() ; t_itr != table_index.end(); ++t_itr){
 cout << (*t_itr).first <<" "<< (*t_itr).second->value << endl;
 }

 }
 */

/*
 void wal_engine::loader(){
 size_t ret;
 stringstream buffer_stream;
 string buffer;
 int rc = -1;

 for(int i=0 ; i<conf.num_keys ; i++){
 int key = i;

 char* value = new char[conf.sz_value];
 random_string(value, conf.sz_value);
 record* after_image = new record(key, value);

 {
 //wrlock(&table_access);

 table_index[key] = after_image;

 //unlock(&table_access);
 }

 // Add log entry
 stmt t(0, "Insert", key, value);

 entry e(t, NULL, after_image);
 undo_log.push(e);
 }

 // sync
 undo_log.write();
 }
 */

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

/*
 void wal_engine::snapshot(){
 int rc = -1;

 std::chrono::time_point<std::chrono::high_resolution_clock> start, finish;
 std::chrono::duration<double> elapsed_seconds;

 start = std::chrono::high_resolution_clock::now();

 logger snapshotter ;
 snapshotter.set_path(conf.fs_path+"./snapshot", "w");

 unordered_map<unsigned int, record*>::const_iterator t_itr;

 for (t_itr = table_index.begin() ; t_itr != table_index.end(); ++t_itr){
 stmt t(0, "", -1, NULL);
 record* tuple = (*t_itr).second;

 entry e(t, tuple, NULL);
 snapshotter.push(e);
 }

 // sync
 snapshotter.write();
 snapshotter.close();

 // Add log entry
 stmt t(0, "Snapshot", -1, NULL);
 entry e(t, NULL, NULL);
 undo_log.push(e);

 // sync
 undo_log.write();

 finish = std::chrono::high_resolution_clock::now();
 elapsed_seconds = finish - start;
 std::cout<<"Snapshot  duration: "<< elapsed_seconds.count()<<endl;
 }

 void wal_engine::recovery(){

 // Clear stuff
 undo_log.close();
 table_index.clear();

 cout<<"Read snapshot"<<endl;

 // Read snapshot
 logger reader;
 reader.set_path(conf.fs_path+"./snapshot", "r");

 unsigned int key = -1;
 char txn_type[10];

 while(1){
 char* value =  new char[conf.sz_value];
 if(fscanf(reader.log_file, "%u %s ", &key, value) != EOF){
 stmt t(0, "Insert", key, value);
 insert(t);
 }
 else{
 break;
 }
 }

 reader.close();

 cout<<"Apply log"<<endl;

 ifstream scanner(conf.fs_path+"./log");
 string line;
 bool apply = false;
 char before_val[conf.sz_value];
 unsigned int before_key, after_key;

 if (scanner.is_open()) {

 while (getline(scanner, line)) {
 if(apply == false){
 sscanf(line.c_str(), "%s", txn_type);
 if(strcmp(txn_type, "Snapshot") ==0)
 apply = true;
 }
 // Apply log
 else{
 sscanf(line.c_str(), "%s", txn_type);

 // Update
 if(strcmp(txn_type, "Update") ==0){
 char* after_val=  new char[conf.sz_value];
 sscanf(line.c_str(), "%s %u %s %u %s ", txn_type, &before_key, before_val, &after_key, after_val);

 stmt t(0, "Update", after_key, after_val);
 update(t);
 }

 }
 }

 scanner.close();
 }

 }
 */
