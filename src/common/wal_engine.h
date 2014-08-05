#ifndef WAL_ENGINE_H_
#define WAL_ENGINE_H_

#include <string>
#include <sstream>
#include <atomic>
#include <thread>

#include "engine.h"
#include "nstore.h"
#include "transaction.h"
#include "record.h"
#include "utils.h"
#include "database.h"
#include "pthread.h"
#include "logger.h"

using namespace std;

class wal_engine : public engine {
 public:
  wal_engine(const config& _conf, bool _read_only = false);
  ~wal_engine();

  std::string select(const statement& st);
  int update(const statement& st);
  int insert(const statement& t);
  int remove(const statement& t);

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
  int engine_txn_id;

  const int active_txn_threshold = 10;
};

#endif /* WAL_ENGINE_H_ */
