#ifndef DATABASE_H_
#define DATABASE_H_

#include "table.h"
#include "plist.h"

using namespace std;

class database {
 public:

  ~database() {
    // clean up tables
    vector<table*> table_vec = tables->get_data();
    for (table* table : table_vec)
      delete PMEM(table);

    delete PMEM(tables);
  }

  plist<table*>* tables;
};

#endif /* DATABASE_H_ */
