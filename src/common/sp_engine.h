#ifndef SP_ENGINE_H_
#define SP_ENGINE_H_

#include <vector>
#include <string>
#include <thread>
#include <queue>
#include <sstream>

#include "engine.h"
#include "nstore.h"
#include "transaction.h"
#include "record.h"
#include "utils.h"
#include "workload.h"
#include "database.h"
#include "pthread.h"
#include "ptreap.h"

using namespace std;

class sp_engine : public engine {
 public:
  sp_engine(const config& _conf);
  ~sp_engine();

  std::string select(const statement& st);
  void update(const statement& st);
  void insert(const statement& t);
  void remove(const statement& t);

  void generator(const workload& load, bool stats);
  void runner();
  void execute(const transaction& t);

  void recovery();

  //private:
  const config& conf;
  database* db;
  std::vector<std::thread> executors;

  std::hash<std::string> hash_fn;

  pthread_rwlock_t txn_queue_rwlock = PTHREAD_RWLOCK_INITIALIZER;
  std::queue<transaction> txn_queue;
  std::atomic<bool> done;
};

#endif /* SP_ENGINE_H_ */
