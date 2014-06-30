#ifndef TABLE_H_
#define TABLE_H_

#include "record.h"
#include "table_index.h"
#include "plist.h"

using namespace std;

class table {
 public:
  table(const std::string& name, unsigned int _num_indices)
      : num_indices(_num_indices),
        indices(NULL) {

    size_t len = name.size();
    char* tmp = new char[len + 1];
    memcpy(tmp, name.c_str(), len + 1);

    table_name = OFF(tmp);
    pmemalloc_activate(pmp, table_name);
  }

  ~table() {
    if (indices != NULL) {
      // clean up table indices
      vector<table_index*> index_vec = PMEM(indices)->get_data();
      for (table_index* index : index_vec)
        delete PMEM(index);

      delete PMEM(indices);
    }

    if (table_name != NULL)
      delete PMEM(table_name);
  }

  //private:
  char* table_name;
  unsigned int num_indices;
  plist<table_index*>* indices;
};

#endif /* TABLE_H_ */
