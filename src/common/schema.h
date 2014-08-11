#ifndef SCHEMA_H_
#define SCHEMA_H_

#include "libpm.h"
#include "field.h"

#include <iostream>
#include <vector>
#include <iomanip>

using namespace std;

class schema {
 public:
  schema(vector<field_info> _columns)
      : columns(NULL),
        ser_len(0),
        deser_len(0){

    num_columns = _columns.size();
    columns = new field_info[num_columns];
    unsigned int itr;

    for (itr = 0; itr < num_columns; itr++) {
      columns[itr] = _columns[itr];
      ser_len += columns[itr].ser_len;
      deser_len += columns[itr].deser_len;
    }

    pmemalloc_activate(columns);
  }

  ~schema() {
    delete[] columns;
  }

  void display() {
    unsigned int itr;

    for (itr = 0; itr < num_columns; itr++) {
      cout << std::setw(20);
      cout << "offset    : " << columns[itr].offset << " ";
      cout << "ser_len   : " << columns[itr].ser_len << " ";
      cout << "deser_len : " << columns[itr].deser_len << " ";
      cout << "type      : " << (int) columns[itr].type << " ";
      cout << "inlined   : " << (int) columns[itr].inlined << " ";
      cout << "enabled   : " << (int) columns[itr].enabled << " ";
      cout << endl;
    }

    cout << endl;
  }

  field_info* columns;
  size_t ser_len;
  size_t deser_len;
  unsigned int num_columns;
};

#endif /* SCHEMA_H_ */
