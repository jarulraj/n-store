#ifndef WAL_H_
#define WAL_H_

#include <vector>
#include <string>
#include <thread>
#include <memory>
#include <mutex>
#include <condition_variable>

#include "engine.h"
#include "logger.h"
#include "nstore.h"
#include "transaction.h"
#include "record.h"
#include "utils.h"

using namespace std;

#define CAPACITY 1024

class wal_engine : public engine {
 public:
  wal_engine(config _conf)
      : conf(_conf),
        ready(false) {


  }

  void runner();

  std::string read(statement* t);
  int update(statement* t);

  int test();

  // Custom functions
  void group_commit();
  int insert(statement* t);

  void check();

  void snapshot();
  void recovery();

 private:
  config conf;

  std::mutex gc_mutex;
  std::condition_variable cv;bool ready;

  logger undo_log;

  boost::lockfree::queue<statement*, boost::lockfree::capacity<CAPACITY> > queue;
  std::atomic<bool> done;

};

#endif /* WAL_H_ */
