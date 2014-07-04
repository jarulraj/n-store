#ifndef DATABASE_H_
#define DATABASE_H_

#include "table.h"

using namespace std;

class database {
 public:
  database()
      : tables(NULL),
        log(NULL) {
  }

  ~database() {
    // clean up tables
    vector<table*> table_vec = tables->get_data();
    for (table* table : table_vec)
      delete table;

    delete tables;
    delete log;
  }

  plist<table*>* tables;
  plist<char*>* log;
};

#endif /* DATABASE_H_ */
