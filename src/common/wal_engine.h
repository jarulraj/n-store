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
#include "workload.h"

using namespace std;

class wal_engine : public engine {
 public:
  wal_engine(unsigned int _part_id, const config& _conf, workload& _load)
      : partition_id(_part_id),
        conf(_conf),
        load(_load),
        ready(false) {
  }

  void runner();

  std::string select(const statement* st);
  int update(const statement* st);
  int insert(const statement* t);

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
  workload& load;

  std::mutex gc_mutex;
  std::condition_variable cv;bool ready;

  logger undo_log;

  std::queue<message> msg_queue;
  std::atomic<bool> done;

};

#endif /* WAL_H_ */
