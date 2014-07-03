#include <iostream>
#include <cstring>
#include <string>

#include <vector>
#include <thread>
#include <mutex>

#include "libpm.h"
#include "plist.h"
#include "ptree.h"

using namespace std;

extern struct static_info* sp;

class tab_index_ {
 public:
  tab_index_(std::string str)
      : map(NULL) {

    size_t len = str.size();
    data = new char[len + 1];

    memcpy(data, str.c_str(), len + 1);
    pmemalloc_activate(data);

  }

  std::string get_name() {
    return std::string(data);
  }

  ~tab_index_() {
    delete data;
  }

  char* data;
  ptree<int, int>* map;
};

class tab_ {
 public:
  tab_(int _val)
      : table_id(_val),
        indices(NULL) {
  }

  std::string get_id() {
    return std::to_string(table_id);
  }

  ~tab_() {
    indices->clear();
  }

  int table_id;
  plist<tab_index_*>* indices;
};

class dbase_ {
 public:
  dbase_(std::string str)
      : tables(NULL) {

    size_t len = str.size();
    data = new char[len + 1];

    memcpy(data, str.c_str(), len + 1);
    pmemalloc_activate(data);

    val = 2;
  }

  ~dbase_() {
    tables->clear();

    delete data;
  }

  int val;
  char* data;
  plist<tab_*>* tables;
};

int main(int argc, char *argv[]) {
  const char* path = "./testfile";

  long pmp_size = 10 * 1024 * 1024;
  if ((pmp = pmemalloc_init(path, pmp_size)) == NULL)
    cout << "pmemalloc_init on :" << path << endl;

  sp = (struct static_info *) pmemalloc_static_area();

  dbase_* db = NULL;

  if (sp->init == 0) {
    cout << "Initialization mode" << endl;
    cout << "Database :" << db << endl;

    db = new dbase_("ycsb");
    sp->ptrs[0] = db;
    pmemalloc_activate(db);

    // TABLES
    db->val = 1023;
    plist<tab_*>* tables = new plist<tab_*>(&sp->ptrs[1], &sp->ptrs[2]);
    pmemalloc_activate(tables);
    db->tables = tables;

    tab_* usertable = new tab_(123);
    pmemalloc_activate(usertable);
    tables->push_back(usertable);

    printf("Table Init : %s \n", tables->at(0)->get_id().c_str());

    // INDICES
    plist<tab_index_*>* indices = new plist<tab_index_*>(&sp->ptrs[3],
                                                         &sp->ptrs[4]);
    pmemalloc_activate(indices);
    usertable->indices = indices;

    tab_index_* usertable_index = new tab_index_("usertable_index");
    pmemalloc_activate(usertable_index);
    indices->push_back(usertable_index);

    tab_index_* usertable_index_2 = new tab_index_("usertable_index_2");
    pmemalloc_activate(usertable_index_2);
    indices->push_back(usertable_index_2);

    ptree<int, int>* usertable_index_map = new ptree<int, int>(&sp->ptrs[5]);
    pmemalloc_activate(usertable_index_map);
    usertable_index->map = usertable_index_map;

    usertable_index_map->insert(1, 10);
    usertable_index_map->insert(2, 20);

    usertable_index_map->display();

    tables->display();

    cout << "Index 1: " << tables->at(0)->indices->at(0)->map << endl;

    sp->init = 1;
  } else {
    cout << "Recovery mode ::" << endl;
    db = (dbase_*) sp->ptrs[0];
    cout << "Database :" << db << endl;

    db->tables->display();

    cout << "Index 2: " << db->tables->at(0)->indices->at(0)->map << endl;

    db->tables->at(0)->indices->at(0)->map->insert(3, 30);
    db->tables->at(0)->indices->at(0)->map->insert(1, 23);

    db->tables->at(0)->indices->at(0)->map->display();

    tab_* usertable_3 = new tab_(14);
    pmemalloc_activate(usertable_3);
    db->tables->push_back(usertable_3);

    db->tables->display();

  }

  return 0;
}

