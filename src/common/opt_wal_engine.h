#pragma once

#include <string>
#include <sstream>
#include <atomic>

#include "engine_api.h"
#include "config.h"
#include "transaction.h"
#include "record.h"
#include "utils.h"
#include "database.h"
#include "pthread.h"
#include "plist.h"
#include "timer.h"
#include "serializer.h"

namespace storage {

class opt_wal_engine : public engine_api {
 public:
  opt_wal_engine(const config& _conf, database* _db, bool _read_only, unsigned int _tid);
  ~opt_wal_engine();

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

  plist<char*>* pm_log;
  std::hash<std::string> hash_fn;

  std::stringstream entry_stream;
  std::string entry_str;
  std::vector<void*> commit_free_list;
  pthread_rwlock_t log_rwlock = PTHREAD_RWLOCK_INITIALIZER;

  std::atomic_bool ready;
  int looper = 0;

  bool read_only = false;
  unsigned int tid;

  serializer sr;
};

}

