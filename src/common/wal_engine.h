#pragma once

#include <string>
#include <sstream>
#include <atomic>
#include <thread>
#include <fstream>

#include "engine_api.h"
#include "config.h"
#include "transaction.h"
#include "record.h"
#include "utils.h"
#include "database.h"
#include "pthread.h"
#include "logger.h"
#include "timer.h"
#include "serializer.h"

namespace storage {

class wal_engine : public engine_api {
 public:
  wal_engine(const config& _conf, database* _db, bool _read_only, unsigned int _tid);
  ~wal_engine();

  std::string select(const statement& st);
  int update(const statement& st);
  int insert(const statement& t);
  int remove(const statement& t);

  void load(const statement& t);

  void group_commit();
  void txn_begin();
  void txn_end(bool commit);
  void recovery();

  //private:
  const config& conf;
  database* db;

  logger fs_log;
  std::hash<std::string> hash_fn;
  std::stringstream entry_stream;
  std::string entry_str;

  std::thread gc;
  std::atomic_bool ready;

  bool read_only = false;
  unsigned int tid;
  serializer sr;
};

}

