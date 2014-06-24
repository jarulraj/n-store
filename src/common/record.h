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
    data = new field*[num_fields];
  }

  ~record() {
    unsigned int itr;

    // clean up fields
    for (itr = 0; itr < num_fields; itr++) {
      delete data[itr];
    }

    delete[] data;
  }

  unsigned int num_fields;
  field** data;

};

#endif /* RECORD_H_ */
