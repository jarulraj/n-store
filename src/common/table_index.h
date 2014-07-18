#ifndef TABLE_INDEX_H_
#define TABLE_INDEX_H_

#include "schema.h"
#include "record.h"
#include "pbtree.h"

using namespace std;

class table_index {
 public:

  table_index(schema* _sptr, unsigned int _num_fields)
      : sptr(_sptr),
        num_fields(_num_fields),
        map(NULL),
        lsm_map(NULL){
  }

  ~table_index() {
    delete sptr;
    delete map;
    delete lsm_map;
  }

  schema* sptr;
  unsigned int num_fields;

  pbtree<unsigned long, record*>* map;

  pbtree<unsigned long, off_t>* lsm_map;
};

#endif /* TABLE_INDEX_H_ */
