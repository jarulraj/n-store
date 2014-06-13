#ifndef TABLE_H_
#define TABLE_H_

#include <unordered_map>
#include "record.h"
#include "key.h"

using namespace std;

class table {
 public:
  table(unsigned long _table_id)
      : table_id(_table_id) {
  }

  //private:
  unsigned long table_id;

  // WAL
  unordered_map<key, record*, key_hasher> table_index;
  pthread_rwlock_t table_access = PTHREAD_RWLOCK_INITIALIZER;

};

#endif /* TABLE_H_ */
