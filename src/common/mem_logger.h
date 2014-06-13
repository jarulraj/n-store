#ifndef MEM_LOGGER_H_
#define MEM_LOGGER_H_

#include <stdlib.h>
#include <unistd.h>

#include <mutex>
#include <vector>

#include "record.h"

using namespace std;

class mem_entry {
 public:
  mem_entry(transaction _txn, char* _before_image, char* _after_image)
      : transaction(_txn),
        before_image(_before_image),
        after_image(_after_image) {
  }

  //private:
  transaction transaction;
  char* before_image;
  char* after_image;
};

class mem_logger {
 public:

  void push(const mem_entry& e) {
    std::lock_guard<std::mutex> lock(log_access);

    log_queue.push_back(e);
  }

  void clear() {
    log_queue.clear();
  }

 private:

  std::mutex log_access;
  vector<mem_entry> log_queue;
};

#endif /* MEM_LOGGER_H_ */
