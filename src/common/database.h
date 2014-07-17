#ifndef DATABASE_H_
#define DATABASE_H_

#include "table.h"
#include "plist.h"
#include "cow_pbtree.h"

using namespace std;

class database {
 public:
  database()
      : tables(NULL),
        log(NULL),
        dirs(NULL) {
  }

  ~database() {
    // clean up tables
    vector<table*> table_vec = tables->get_data();
    for (table* table : table_vec)
      delete table;

    delete tables;
    delete log;
  }

  // WAL
  plist<table*>* tables;

  // ARIES
  plist<char*>* log;

  // SP
  cow_pbtree* dirs;
};

#endif /* DATABASE_H_ */
