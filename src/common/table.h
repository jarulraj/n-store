#ifndef TABLE_H_
#define TABLE_H_

#include "record.h"
#include "table_index.h"

using namespace std;

class table {
 public:
  table(const std::string& name, unsigned int _num_indices)
      : num_indices(_num_indices) {

    size_t len = name.size();
    table_name = new char[len + 1];
    memcpy(table_name, name.c_str(), len + 1);

    indices = new table_index*[num_indices];
  }

  ~table() {
    unsigned int itr;

    // clean up table indices
    for (itr = 0; itr < num_indices; itr++) {
      delete indices[itr];
    }

    delete[] indices;

    delete table_name;
  }

  //private:
  char* table_name;
  unsigned int num_indices;
  table_index** indices;
};

#endif /* TABLE_H_ */
