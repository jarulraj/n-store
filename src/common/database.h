#ifndef DATABASE_H_
#define DATABASE_H_

#include "table.h"

using namespace std;

class database {
 public:

  database(unsigned int _num_tables)
      : num_tables(_num_tables) {
    tables = new table*[num_tables];
  }

  ~database() {
    unsigned int itr;

    // clean up tables
    for (itr = 0; itr < num_tables; itr++) {
      delete tables[itr];
    }

    delete[] tables;
  }

  unsigned int num_tables;
  table** tables;

};

#endif /* DATABASE_H_ */
