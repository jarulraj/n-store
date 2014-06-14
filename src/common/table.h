#ifndef TABLE_H_
#define TABLE_H_

#include <unordered_map>
#include "record.h"
#include "key.h"

using namespace std;

class table {
 public:
  table(std::string _name)
      : table_name(_name) {
  }

  //private:
  std::string table_name;

  // WAL
  unordered_map<std::string, record*> table_index;
  pthread_rwlock_t table_access = PTHREAD_RWLOCK_INITIALIZER;

};

#endif /* TABLE_H_ */
