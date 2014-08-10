#ifndef TABLE_H_
#define TABLE_H_

#include "schema.h"
#include "table_index.h"
#include "plist.h"
#include "storage.h"

using namespace std;

class table {
 public:
  table(const std::string& name_str, schema* _sptr, unsigned int _num_indices,
        config& conf)
      : table_name(NULL),
        sptr(_sptr),
        max_tuple_size(_sptr->deser_len),
        num_indices(_num_indices),
        indices(NULL),
        pm_data(NULL) {

    size_t len = name_str.size();
    table_name = new char[len + 1];
    memcpy(table_name, name_str.c_str(), len + 1);
    pmemalloc_activate(table_name);

    pm_data = new plist<record*>(&conf.sp->ptrs[conf.sp->itr++],
                                 &conf.sp->ptrs[conf.sp->itr++]);
    pmemalloc_activate(pm_data);

    indices = new plist<table_index*>(&conf.sp->ptrs[conf.sp->itr++],
                                      &conf.sp->ptrs[conf.sp->itr++]);
    pmemalloc_activate(indices);

  }

  ~table() {
    delete table_name;
    delete sptr;

    if (indices != NULL) {
      // clean up table indices
      vector<table_index*> index_vec = indices->get_data();
      for (table_index* index : index_vec)
        delete index;

      delete indices;
    }
  }

  //private:
  char* table_name;
  schema* sptr;
  size_t max_tuple_size;
  unsigned int num_indices;

  plist<table_index*>* indices;

  storage fs_data;

  plist<record*>* pm_data;
};

#endif /* TABLE_H_ */
