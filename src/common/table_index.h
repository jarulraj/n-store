#ifndef TABLE_INDEX_H_
#define TABLE_INDEX_H_

#include <unordered_map>
#include "record.h"

using namespace std;

class table_index {
 public:

  table_index(unsigned int _num_fields, bool* _key)
      : num_fields(_num_fields),
        key(_key) {
    key = new bool[num_fields];
    memcpy(key, _key, num_fields);
  }

  ~table_index() {
    delete key;
  }

  unsigned int num_fields;
  bool* key;

  unordered_map<std::string, record*> index;

};

#endif /* TABLE_INDEX_H_ */
