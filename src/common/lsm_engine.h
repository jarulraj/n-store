#ifndef LSM_ENGINE_H_
#define LSM_ENGINE_H_

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

using namespace std;

class lsm_engine : public engine {
 public:
  lsm_engine(const config& _conf);
  ~lsm_engine();

  std::string select(const statement& st);
  void update(const statement& st);
  void insert(const statement& t);
  void remove(const statement& t);

  void generator(const workload& load, bool stats);
  void runner();
  void execute(const transaction& t);

  void group_commit();
  void merge();
  void recovery();

  //private:
  const config& conf;
  database* db;
  std::vector<std::thread> executors;

  logger fs_log;
  std::hash<std::string> hash_fn;

  std::stringstream entry_stream;
  std::string entry_str;

  pthread_rwlock_t txn_queue_rwlock = PTHREAD_RWLOCK_INITIALIZER;
  std::queue<transaction> txn_queue;
  std::atomic_bool done;

  pthread_rwlock_t merge_rwlock = PTHREAD_RWLOCK_INITIALIZER;

  std::atomic_bool ready;
  unsigned long merge_looper = 0;

};

#endif /* LSM_ENGINE_H_ */
