#ifndef DATABASE_H_
#define DATABASE_H_

#include "table.h"

using namespace std;

class database {
 public:
  database()
      : tables(NULL),
        log(NULL),
        commit_free_list(NULL) {
  }

  ~database() {
    // clean up tables
    vector<table*> table_vec = tables->get_data();
    for (table* table : table_vec)
      delete table;

    delete tables;
    delete log;
    delete commit_free_list;
  }

  plist<table*>* tables;
  plist<char*>* log;
  plist<void*>* commit_free_list;
};

#endif /* DATABASE_H_ */
