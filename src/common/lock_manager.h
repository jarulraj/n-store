#ifndef LOCK_MANAGER_H_
#define LOCK_MANAGER_H_

#include  <map>

#include "utils.h"
#include "pthread.h"

using namespace std;

class lock_manager {
 public:

  int tuple_lock(unsigned int table_id, unsigned int tuple_id) {
    unsigned long hash = hasher(table_id, tuple_id);

    wrlock(&lock_table_rwlock);
    if (lock_table[hash] == false) {
      lock_table[hash] = true;
      return 0;
    }
    unlock(&lock_table_rwlock);

    return -1;
  }

  int tuple_unlock(unsigned int table_id, unsigned int tuple_id) {
    unsigned long hash = hasher(table_id, tuple_id);

    wrlock(&lock_table_rwlock);
    if (lock_table[hash] == true) {
      lock_table[hash] = false;
      return 0;
    }
    unlock(&lock_table_rwlock);

    return -1;
  }

  pthread_rwlock_t lock_table_rwlock = PTHREAD_RWLOCK_INITIALIZER;;
  std::map<unsigned long, bool> lock_table;
};

#endif /* LOCK_MANAGER_H_ */
