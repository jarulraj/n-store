#ifndef TABLE_INDEX_H_
#define TABLE_INDEX_H_

#include "schema.h"
#include "record.h"
#include "pbtree.h"
#include "nstore.h"

using namespace std;

class table_index {
 public:

  table_index(schema* _sptr, unsigned int _num_fields, config& conf,
              struct static_info* sp)
      : sptr(_sptr),
        num_fields(_num_fields),
        pm_map(NULL),
        off_map(NULL) {

    pm_map = new pbtree<unsigned long, record*>(&sp->ptrs[sp->itr++]);
    pmemalloc_activate(pm_map);

    off_map = new pbtree<unsigned long, off_t>(&sp->ptrs[sp->itr++]);
    pmemalloc_activate(off_map);

    if (conf.etype == engine_type::WAL || conf.etype == engine_type::LSM) {
      pm_map->disable_persistence();
      off_map->disable_persistence();
    }
  }

  ~table_index() {
    delete sptr;
    delete pm_map;
    delete off_map;
  }

  schema* sptr;
  unsigned int num_fields;

  pbtree<unsigned long, record*>* pm_map;
  pbtree<unsigned long, off_t>* off_map;
};

#endif /* TABLE_INDEX_H_ */
