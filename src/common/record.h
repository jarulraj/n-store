#ifndef RECORD_H_
#define RECORD_H_

#include <iostream>
#include <string>

#include "field.h"

using namespace std;

class record {
 public:
  record(unsigned int _num_fields)
      : num_fields(_num_fields) {
    fields = new field*[num_fields];
  }

  ~record() {
    unsigned int itr;

    // clean up fields
    for (itr = 0; itr < num_fields; itr++) {
      delete fields[itr];
    }

    delete[] fields;
  }

  unsigned int num_fields;
  field** fields;

};

#endif /* RECORD_H_ */
