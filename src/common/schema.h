#ifndef SCHEMA_H_
#define SCHEMA_H_

#include "libpm.h"
#include "field.h"

#include <iostream>
#include <vector>

using namespace std;

class schema {
 public:
  schema(vector<field_info> _columns, size_t _len)
      : columns(NULL),
        len(_len) {

    num_columns = _columns.size();
    columns = new field_info[num_columns];
    unsigned int itr;

    for (itr = 0; itr < num_columns; itr++) {
      columns[itr] = _columns[itr];
    }

    pmemalloc_activate(columns);
  }

  ~schema() {
    delete[] columns;
  }

  size_t len;
  unsigned int num_columns;
  field_info* columns;
};


#endif /* SCHEMA_H_ */
