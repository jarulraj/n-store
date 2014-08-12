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
  database(config conf, struct static_info* sp, unsigned int tid)
      : tables(NULL),
        log(NULL),
        dirs(NULL) {

    sp->itr++;

    // TABLES
    plist<table*>* _tables = new plist<table*>(&sp->ptrs[sp->itr++],
                                               &sp->ptrs[sp->itr++]);
    pmemalloc_activate(_tables);
    tables = _tables;

    // LOG
    log = new plist<char*>(&sp->ptrs[sp->itr++], &sp->ptrs[sp->itr++]);
    pmemalloc_activate(log);

    // DIRS
    if (conf.etype == engine_type::SP) {
      dirs = new cow_pbtree(
          false, (conf.fs_path + std::to_string(tid) + "_" + "cow.nvm").c_str(),
          NULL);
      // No activation
    }

    if (conf.etype == engine_type::OPT_SP) {
      dirs = new cow_pbtree(true, NULL, &sp->ptrs[sp->itr++]);
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

  void reset(config& conf, unsigned int tid) {

    if (conf.etype == engine_type::SP) {
      dirs = new cow_pbtree(
          false, (conf.fs_path + std::to_string(tid) + "_" + "cow.nvm").c_str(),
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
  plist<char*>* log;

  // SP and OPT_SP
  cow_pbtree* dirs;
};

#endif /* DATABASE_H_ */
