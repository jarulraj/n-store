#ifndef TABLE_H_
#define TABLE_H_

#include "record.h"
#include "table_index.h"

using namespace std;

class table {
 public:
  table(std::string _name)
      : table_name(_name) {
  }

  //private:
  std::string table_name;
  vector<table_index*> indices;
};

#endif /* TABLE_H_ */
