#ifndef RECORD_H_
#define RECORD_H_

#include <iostream>
#include <string>

#include "field.h"

using namespace std;

class record {

 public:
  vector<field*> data;
  unsigned int batch_id[2];
  char* location[2];

};

#endif /* RECORD_H_ */
