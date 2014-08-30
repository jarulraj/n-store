#pragma once

#include  <map>
#include "utils.h"

namespace storage {

struct lock_t {
  lock_t()
      : locked(false),
        writer(false),
        readers(0) {
  }

  bool locked;
  bool writer;
  int readers;
};

class lock_manager {
 public:

  inline int tuple_rdlock(const unsigned long hash) {

    wrlock(&lock_table_rwlock);
    if (!lock_table[hash].writer) {
      lock_table[hash].readers++;
      lock_table[hash].locked = true;
      unlock(&lock_table_rwlock);
      return 0;
    }
    unlock(&lock_table_rwlock);

    //abort++;
    return -1;
  }

  inline int tuple_wrlock(const unsigned long hash) {

    wrlock(&lock_table_rwlock);
    if (lock_table[hash].readers == 0) {
      lock_table[hash].writer = true;
      lock_table[hash].locked = true;
      unlock(&lock_table_rwlock);
      return 0;
    }
    unlock(&lock_table_rwlock);

    //abort++;
    return -1;
  }

  inline int tuple_unlock(const unsigned long hash) {

    wrlock(&lock_table_rwlock);
    if (lock_table[hash].locked) {
      if (lock_table[hash].readers != 0) {
        lock_table[hash].readers--;

        if (lock_table[hash].readers == 0)
          lock_table[hash].locked = false;
      } else {
        lock_table[hash].locked = false;
        lock_table[hash].writer = false;
      }
      unlock(&lock_table_rwlock);
      return 0;
    }
    unlock(&lock_table_rwlock);

    return -1;
  }

  pthread_rwlock_t lock_table_rwlock = PTHREAD_RWLOCK_INITIALIZER;;
  std::map<unsigned long, lock_t> lock_table;

  unsigned int abort = 0;
};

}
