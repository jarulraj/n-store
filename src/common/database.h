#ifndef DATABASE_H_
#define DATABASE_H_

#include "nstore.h"
#include "table.h"
#include "plist.h"
#include "cow_pbtree.h"

using namespace std;

class database {
 public:
  database(const config& _conf)
      : tables(NULL),
        log(NULL),
        dirs(NULL),
        conf(_conf) {

    conf.sp->itr++;

    // TABLES
    plist<table*>* _tables = new plist<table*>(&conf.sp->ptrs[conf.sp->itr++],
                                               &conf.sp->ptrs[conf.sp->itr++]);
    pmemalloc_activate(_tables);
    tables = _tables;

    // LOG
    plist<char*>* _log = new plist<char*>(&conf.sp->ptrs[conf.sp->itr++],
                                          &conf.sp->ptrs[conf.sp->itr++]);
    pmemalloc_activate(_log);
    log = _log;

    // DIRS
    if (conf.etype == engine_type::SP) {
      cow_pbtree* _dirs = new cow_pbtree(false,
                                         (conf.fs_path + "cow.db").c_str(),
                                         NULL);
      dirs = _dirs;
      // No activation
    }

    if (conf.etype == engine_type::OPT_SP) {
      cow_pbtree* _dirs = new cow_pbtree(true, NULL,
                                         &conf.sp->ptrs[conf.sp->itr++]);
      pmemalloc_activate(_dirs);
      dirs = _dirs;
    }
  }

  ~database() {
    // clean up tables
    vector<table*> table_vec = tables->get_data();
    for (table* table : table_vec)
      delete table;

    delete tables;
    delete log;
  }

  const config& conf;
  plist<table*>* tables;
  plist<char*>* log;

  // SP and OPT_SP
  cow_pbtree* dirs;
};

#endif /* DATABASE_H_ */
