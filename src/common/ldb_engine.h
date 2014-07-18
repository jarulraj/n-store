#ifndef LDB_ENGINE_H_
#define LDB_ENGINE_H_

#include <vector>
#include <string>
#include <thread>
#include <queue>
#include <sstream>
#include <atomic>

#include "engine.h"
#include "nstore.h"
#include "transaction.h"
#include "record.h"
#include "utils.h"
#include "workload.h"
#include "database.h"
#include "pthread.h"
#include "logger.h"
#include "plist.h"

using namespace std;

class ldb_engine : public engine {
 public:
  ldb_engine(const config& _conf);
  ~ldb_engine();

  std::string select(const statement& st);
  void update(const statement& st);
  void insert(const statement& t);
  void remove(const statement& t);

  void generator(const workload& load, bool stats);
  void runner();
  void execute(const transaction& t);

  void group_commit();
  void recovery();

  //private:
  const config& conf;
  database* db;
  std::vector<std::thread> executors;

  plist<char*>* pm_log;
  std::hash<std::string> hash_fn;

  std::stringstream entry_stream;
  std::string entry_str;

  pthread_rwlock_t txn_queue_rwlock = PTHREAD_RWLOCK_INITIALIZER;
  std::queue<transaction> txn_queue;
  std::atomic_bool done;

  std::atomic_bool ready;
};

#endif /* LDB_ENGINE_H_ */
