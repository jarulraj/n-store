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

  void* operator new(size_t sz) throw (bad_alloc) {
    if (persistent) {
      void* ret = pmem_new(sz);
      pmemalloc_activate(ret);
      return ret;
    } else
      return ::operator new(sz);
  }

  void operator delete(void *p) throw () {
    if (persistent)
      pmem_delete(p);
    else
      ::operator delete(p);
  }

  schema* sptr;
  unsigned int num_fields;
  static bool persistent;

  ptree<unsigned long, record*>* map;
};

#endif /* TABLE_INDEX_H_ */
