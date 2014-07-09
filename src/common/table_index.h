#ifndef TABLE_INDEX_H_
#define TABLE_INDEX_H_

#include "schema.h"
#include "record.h"
#include "ptree.h"

using namespace std;

class table_index {
 public:

  table_index(schema* _sptr, unsigned int _num_fields)
      : sptr(_sptr),
        num_fields(_num_fields),
        map(NULL) {
  }

  ~table_index() {
    delete sptr;
    delete map;
  }

  schema* sptr;
  unsigned int num_fields;

  ptree<unsigned long, record*>* map;
};

#endif /* TABLE_INDEX_H_ */
