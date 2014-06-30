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

    bool* key = new bool[num_fields];
    memcpy(key, _key, num_fields);
    pmemalloc_activate(key);

    cout<<"key ::"<<key<<endl;
  }

  ~table_index() {
    delete map;

    delete key;
  }

  unsigned int num_fields;
  bool* key;

  ptree<unsigned long, record*>* map;
};

#endif /* TABLE_INDEX_H_ */
