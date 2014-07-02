#ifndef WAL_H_
#define WAL_H_

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
#include "message.h"
#include "workload.h"
#include "database.h"
#include "pthread.h"
#include "plist.h"

using namespace std;

class wal_engine : public engine {
 public:
  wal_engine(unsigned int _part_id, const config& _conf, database* _db)
      : partition_id(_part_id),
        conf(_conf),
        db(_db),
        done(false),
        undo_log(db->log),
        entry_len(0) {
  }

  void runner();

  std::string select(const statement& st);
  void update(const statement& st);
  void insert(const statement& t);
  void remove(const statement& t);

  int test();

  // Custom functions
  void group_commit();

  void handle_message(const message& msg);
  void check();

  void snapshot();
  void recovery();

  //private:
  unsigned int partition_id;
  const config& conf;
  database* db;

  plist<char*>* undo_log;
  std::stringstream entry_stream;
  std::string entry_str;
  size_t entry_len;
  std::hash<std::string> hash_fn;

  pthread_rwlock_t msg_queue_rwlock = PTHREAD_RWLOCK_INITIALIZER;
  std::queue<message> msg_queue;
  std::atomic<bool> done;
};

#endif /* WAL_H_ */
