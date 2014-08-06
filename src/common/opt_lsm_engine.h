#ifndef OPT_LSM_ENGINE_H_
#define OPT_LSM_ENGINE_H_

#include <vector>
#include <string>
#include <thread>
#include <sstream>
#include <atomic>
#include <fstream>

#include "engine.h"
#include "nstore.h"
#include "transaction.h"
#include "record.h"
#include "utils.h"
#include "database.h"
#include "pthread.h"
#include "logger.h"
#include "plist.h"
#include "timer.h"

using namespace std;

class opt_lsm_engine : public engine {
 public:
  opt_lsm_engine(const config& _conf, bool _read_only = false);
  ~opt_lsm_engine();

  std::string select(const statement& st);
  int update(const statement& st);
  int insert(const statement& t);
  int remove(const statement& t);

  void load(const statement& st);
  void group_commit();

  void merge(bool force);
  void merge_check();

  void txn_begin();
  void txn_end(bool commit);

  void recovery();

  //private:
  const config& conf;
  database* db;
  std::vector<std::thread> executors;

  plist<char*>* pm_log;
  std::hash<std::string> hash_fn;

  std::stringstream entry_stream;
  std::string entry_str;

  std::vector<void*> commit_free_list;

  std::atomic_bool ready;
  unsigned long merge_looper = 0;

  bool read_only = false;
};

#endif /* OPT_LSM_ENGINE_H_ */
