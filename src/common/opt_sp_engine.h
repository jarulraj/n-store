#ifndef OPT_SP_ENGINE_H_
#define OPT_SP_ENGINE_H_


#include <vector>
#include <string>
#include <thread>
#include <atomic>
#include <sstream>

#include "engine.h"
#include "nstore.h"
#include "transaction.h"
#include "record.h"
#include "utils.h"
#include "database.h"
#include "pthread.h"
#include "cow_pbtree.h"

using namespace std;

class opt_sp_engine : public engine {
 public:
  opt_sp_engine(const config& _conf, bool _read_only = false);
  ~opt_sp_engine();

  std::string select(const statement& st);
  int update(const statement& st);
  int insert(const statement& t);
  int remove(const statement& t);

  void group_commit();

  void txn_begin();
  void txn_end(bool commit);

  void recovery(){}

  //private:
  const config& conf;
  database* db;

  std::hash<std::string> hash_fn;

  std::thread gc;
  pthread_rwlock_t opt_sp_pbtree_rwlock = PTHREAD_RWLOCK_INITIALIZER;
  std::atomic_bool ready;

  cow_btree* bt;
  struct cow_btree_txn* txn_ptr;

  bool read_only = false;
};




#endif /* OPT_SP_ENGINE_H_ */
