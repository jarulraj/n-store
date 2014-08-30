#pragma once

#include <vector>
#include <string>
#include <thread>
#include <atomic>
#include <sstream>

#include "engine_api.h"
#include "config.h"
#include "transaction.h"
#include "record.h"
#include "utils.h"
#include "database.h"
#include "pthread.h"
#include "cow_pbtree.h"
#include "serializer.h"

namespace storage {

class sp_engine : public engine_api {
 public:
  sp_engine(const config& _conf, database* _db, bool _read_only, unsigned int _tid);
  ~sp_engine();

  std::string select(const statement& st);
  int update(const statement& st);
  int insert(const statement& t);
  int remove(const statement& t);

  void load(const statement& st);

  void group_commit();

  void txn_begin();
  void txn_end(bool commit);

  void recovery();

  //private:
  const config& conf;
  database* db;

  std::hash<std::string> hash_fn;
  std::thread gc;
  pthread_rwlock_t gc_rwlock = PTHREAD_RWLOCK_INITIALIZER;
  std::atomic_bool ready;

  struct cow_btree* bt;
  struct cow_btree_txn* txn_ptr;

  bool read_only = false;
  unsigned int tid;

  serializer sr;
};

}

