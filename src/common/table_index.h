#ifndef TABLE_INDEX_H_
#define TABLE_INDEX_H_

#include <unordered_map>
#include "record.h"

#include "libpm.h"
#include "ptree.h"

using namespace std;

class table_index {
 public:

  table_index(unsigned int _num_fields, bool* _key)
      : num_fields(_num_fields),
        key(_key),
        map(NULL) {

    bool* tmp = new bool[num_fields];
    memcpy(tmp, _key, num_fields);

    key = OFF(tmp);
    pmemalloc_activate(pmp, key);
  }

  ~table_index() {
    if (map != NULL)
      delete PMEM(map);

    if (key != NULL)
      delete PMEM(key);
  }

  unsigned int num_fields;
  bool* key;

  ptree<unsigned long, record*>* map;
};

#endif /* TABLE_INDEX_H_ */
