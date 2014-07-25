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
#include "cow_pbtree.h"

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

  void group_commit();
  void recovery();

  //private:
  const config& conf;
  database* db;
  std::vector<std::thread> executors;

  std::hash<std::string> hash_fn;

  pthread_rwlock_t txn_queue_rwlock = PTHREAD_RWLOCK_INITIALIZER;
  std::queue<transaction> txn_queue;
  std::atomic_bool done;

  pthread_rwlock_t cow_pbtree_rwlock = PTHREAD_RWLOCK_INITIALIZER;
  std::atomic_bool ready;

  struct cow_btree* bt;
  struct cow_btree_txn* txn_ptr;

  bool read_only = false;
  int looper = 0;
};

#endif /* SP_ENGINE_H_ */
