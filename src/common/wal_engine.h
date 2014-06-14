#ifndef WAL_H_
#define WAL_H_

#include <vector>
#include <string>
#include <thread>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <queue>

#include "engine.h"
#include "logger.h"
#include "nstore.h"
#include "transaction.h"
#include "record.h"
#include "utils.h"
#include "message.h"

using namespace std;

#define CAPACITY 32

class wal_engine : public engine {
 public:
  wal_engine(unsigned int _part_id, config _conf)
      : partition_id(_part_id),
        conf(_conf),
        ready(false) {
  }

  void runner();

  std::string select(statement* t);
  int update(statement* t);

  int test();

  // Custom functions
  void group_commit();
  int insert(statement* t);

  void handle_message(const message& msg);
  void check();

  void snapshot();
  void recovery();

  //private:
  unsigned int partition_id;
  config conf;

  std::mutex gc_mutex;
  std::condition_variable cv;bool ready;

  logger undo_log;

  std::queue<message> msg_queue;
  std::atomic<bool> done;

};

#endif /* WAL_H_ */
