#pragma once

#include "schema.h"
#include "record.h"
#include "pbtree.h"
#include "config.h"

namespace storage {

class table_index {
 public:

  table_index(schema* _sptr, unsigned int _num_fields, config& conf,
              struct static_info* sp)
      : sptr(_sptr),
        num_fields(_num_fields),
        pm_map(NULL),
        off_map(NULL) {

    pm_map = new ((pbtree<unsigned long, record*>*) pmalloc(sizeof(pbtree<unsigned long, record*>))) \
							pbtree<unsigned long, record*>(&sp->ptrs[get_next_pp()]);
    pmemalloc_activate(pm_map);

    off_map = new ((pbtree<unsigned long, off_t>*) pmalloc(sizeof(pbtree<unsigned long, off_t>))) \
							pbtree<unsigned long, off_t>(&sp->ptrs[get_next_pp()]);
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

}
