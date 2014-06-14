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

int wal_engine::update(statement* t) {
  key key = *(t->rec_key);
  int rc = -1;

  if (t->table_ptr->table_index.count(key) == 0) {
    return -1;
  }

  record* before_image;
  record* after_image = t->rec_ptr;

  before_image = t->table_ptr->table_index.at(key);
  t->table_ptr->table_index[key] = after_image;

  // Add log entry
  entry e(t, before_image, after_image);
  undo_log.push(e);

  return 0;
}

std::string wal_engine::select(statement* t) {
  key key = *(t->rec_key);
  std::string val;

  if (t->table_ptr->table_index.count(key) == 0) {
    return NULL;
  }

  record* r = t->table_ptr->table_index[key];
  val = r->get_string();

  return val;
}

int wal_engine::insert(statement* st) {
  key key = *(st->rec_key);

  // check if key already exists
  if (st->table_ptr->table_index.count(key) != 0) {
    return -1;
  }

  record* after_image = st->rec_ptr;

  st->table_ptr->table_index[key] = after_image;

  cout<<"wal: "<<endl;
  cout<<st->rec_ptr<<endl;

  // Add log entry
  entry e(st, NULL, after_image);
  undo_log.push(e);

  return 0;
}

// RUNNER + LOADER

void wal_engine::handle_message(const message& msg) {
  statement* s_ptr;

  unsigned int s_id = msg.statement_id;
  s_ptr = msg.st_ptr;

  if (s_ptr->op_type == operation_type::Insert) {
    cout << "Insert stmt " << s_id << " on engine " << partition_id << endl;

    //insert(s_ptr);
  } else if (s_ptr->op_type == operation_type::Select) {
    cout << "Select stmt " << s_id << " on engine " << partition_id << endl;

    //select(s_ptr);
  }

}

void wal_engine::runner() {
  int consumer_count;
  message msg;

  cout << "Runner" << endl;

  while (!done) {
    if (!msg_queue.empty()) {
      msg = msg_queue.front();
      msg_queue.pop();

      handle_message(msg);
    }
  }

  while(!msg_queue.empty()){
    msg = msg_queue.front();
    msg_queue.pop();

    handle_message(msg);
  }

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

  undo_log.set_path(conf.fs_path + "./log", "w");

  timespec start, finish;

  //std::cout<<"Loading finished "<<endl;
  //check();

  // Take snapshot
  //snapshot();

  clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &start);

  // Logger start
  std::thread gc(&wal_engine::group_commit, this);
  {
    std::lock_guard<std::mutex> lk(gc_mutex);
    ready = true;
  }
  cv.notify_one();

  // Runner
  std::vector<std::thread> th_group;
  for (int i = 0; i < conf.num_parts; i++)
    th_group.push_back(std::thread(&wal_engine::runner, this));

  for (int i = 0; i < conf.num_parts; i++)
    th_group.at(i).join();

  // Logger end
  ready = false;
  gc.join();

  undo_log.write();

  clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &finish);
  display_stats(start, finish, conf.num_txns);

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
