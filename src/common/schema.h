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

    // XXX Activate columns new []
  }

  ~schema() {
    delete[] columns;
  }

  static void* operator new(size_t sz) throw (bad_alloc) {
    if (persistent) {
      void* ret = pmem_new(sz);
      pmemalloc_activate(ret);
      return ret;
    } else
      return ::operator new(sz);
  }

  static void operator delete(void *p) throw () {
    if (persistent)
      pmem_delete(p);
    else
      ::operator delete(p);
  }

  void display() {
    unsigned int itr;

    for (itr = 0; itr < num_columns; itr++) {
      cout << "offset : " << columns[itr].offset << " ";
      cout << "len : " << columns[itr].len << " ";
      cout << "type : " << (int) columns[itr].type << " ";
      cout << "inlined : " << (int) columns[itr].inlined << " ";
      cout << endl;
    }

    cout << endl;
  }

  size_t len;
  unsigned int num_columns;
  field_info* columns;
  static bool persistent;

};

#endif /* SCHEMA_H_ */
