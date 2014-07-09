#ifndef DATABASE_H_
#define DATABASE_H_

#include "table.h"
#include "plist.h"
#include "ptreap.h"

using namespace std;

class database {
 public:
  database()
      : tables(NULL),
        log(NULL),
        dirs(NULL),
        version(0){
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
  ptreap<unsigned long, record*>* dirs;
  unsigned int version;
};

#endif /* DATABASE_H_ */
