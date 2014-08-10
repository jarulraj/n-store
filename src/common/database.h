#ifndef DATABASE_H_
#define DATABASE_H_

#include "nstore.h"
#include "table.h"
#include "plist.h"
#include "cow_pbtree.h"
#include <set>

using namespace std;

class database {
 public:
  database(config& conf)
      : tables(NULL),
        log(NULL),
        dirs(NULL) {

    conf.sp->itr++;

    // TABLES
    plist<table*>* _tables = new plist<table*>(&conf.sp->ptrs[conf.sp->itr++],
                                               &conf.sp->ptrs[conf.sp->itr++]);
    pmemalloc_activate(_tables);
    tables = _tables;

    // LOG
    log = new plist<plist<char*>*>(&conf.sp->ptrs[conf.sp->itr++],
                                   &conf.sp->ptrs[conf.sp->itr++]);
    pmemalloc_activate(log);

    for (int e_itr = 0; e_itr < conf.num_executors; e_itr++) {
      plist<char*>* _log = new plist<char*>(&conf.sp->ptrs[conf.sp->itr++],
                                            &conf.sp->ptrs[conf.sp->itr++]);
      pmemalloc_activate(_log);
      log->push_back(_log);
    }

    // DIRS
    if (conf.etype == engine_type::SP) {
      dirs = new cow_pbtree(false, (conf.fs_path + "cow.nvm").c_str(),
      NULL);
      // No activation
    }

    if (conf.etype == engine_type::OPT_SP) {
      dirs = new cow_pbtree(true, NULL, &conf.sp->ptrs[conf.sp->itr++]);
      pmemalloc_activate(dirs);
    }
  }

  ~database() {
    // clean up tables
    vector<table*> table_vec = tables->get_data();
    for (table* table : table_vec)
      delete table;

    delete tables;
    delete[] log;
  }

  void reset(config& conf) {

    if (conf.etype == engine_type::SP) {
      dirs = new cow_pbtree(false, (conf.fs_path + "cow.nvm").c_str(),
      NULL);
    }

    // Clear all table data and indices
    if (conf.etype == engine_type::WAL || conf.etype == engine_type::LSM) {
      vector<table*> tab_vec = tables->get_data();

      for (table* tab : tab_vec) {
        tab->pm_data->clear();
        vector<table_index*> indices = tab->indices->get_data();
        for (table_index* index : indices) {
          index->pm_map->clear();
          index->off_map->clear();
        }
      }
    }

  }

  plist<table*>* tables;
  plist<plist<char*>*>* log;

  // SP and OPT_SP
  cow_pbtree* dirs;
  pthread_rwlock_t engine_rwlock = PTHREAD_RWLOCK_INITIALIZER;
};

#endif /* DATABASE_H_ */
