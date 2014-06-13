#ifndef RECORD_H_
#define RECORD_H_

#include <iostream>
#include <string>

using namespace std;

#define DELIM ' '

class record {
 public:

  virtual std::string get_string() = 0;

  virtual ~record() {}
};

class sp_record {
 public:

 private:
  unsigned int batch_id[2];
  char* location[2];
};

#endif /* RECORD_H_ */
