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

void* pmp;
std::mutex pmp_mutex;

#define MAX_PTRS 128
struct static_info {
  int init;
  void* ptrs[MAX_PTRS];
};
struct static_info *sp;

void* operator new(size_t sz) throw (bad_alloc) {
  std::lock_guard<std::mutex> lock(pmp_mutex);
  return PMEM(pmemalloc_reserve(pmp, sz));
}

void operator delete(void *p) throw () {
  std::lock_guard<std::mutex> lock(pmp_mutex);
  pmemalloc_free_absolute(pmp, p);
}

class tab_index_ {
 public:
  tab_index_(std::string str) {

    size_t len = str.size();
    data = OFF(new char[len + 1]);

    memcpy(PMEM(data), str.c_str(), len + 1);
    pmemalloc_activate(pmp, data);

  }

  std::string get_name() {
    return std::string(PMEM(data));
  }

  ~tab_index_() {
    delete PMEM(data);
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
    data = OFF(new char[len + 1]);

    memcpy(PMEM(data), str.c_str(), len + 1);
    pmemalloc_activate(pmp, data);

    val = 2;
  }

  ~dbase_() {
    tables->clear();

    delete PMEM(data);
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

  sp = (struct static_info *) pmemalloc_static_area(pmp);

  dbase_* db;

  // Initialize
  if (sp->init == 0) {
    cout << "Init" << endl;
    cout << "head : " << sp->ptrs[1] << " tail: " << sp->ptrs[2] << endl;

    db = new dbase_("ycsb");
    sp->ptrs[0] = OFF(db);
    pmemalloc_activate(pmp, OFF(db));

    // TABLES
    db->val = 1023;
    db->tables = OFF(new plist<tab_*>(&sp->ptrs[1], &sp->ptrs[2]));
    pmemalloc_activate(pmp, db->tables);

    tab_* usertable = new tab_(123);
    pmemalloc_activate(pmp, OFF(usertable));
    PMEM(db->tables)->push_back(OFF(usertable));

    printf("Table Init : %s \n",
    PMEM(PMEM(db->tables)->at(0))->get_id().c_str());

    // INDICES
    usertable->indices = OFF(new plist<tab_index_*>(&sp->ptrs[3], &sp->ptrs[4]));
    pmemalloc_activate(pmp, usertable->indices);


    tab_index_* usertable_index = new tab_index_("usertable_index");
    pmemalloc_activate_absolute(pmp, usertable_index);
    PMEM(usertable->indices)->push_back(OFF(usertable_index));

    tab_index_* usertable_index_2 = new tab_index_("usertable_index_2");
    pmemalloc_activate_absolute(pmp, usertable_index_2);
    PMEM(usertable->indices)->push_back(OFF(usertable_index_2));

    ptree<int, int>* usertable_index_map = new ptree<int, int>(&sp->ptrs[5]);
    pmemalloc_activate_absolute(pmp, usertable_index_map);
    usertable_index->map = OFF(usertable_index_map);

    PMEM(usertable_index->map)->insert(1, 10);
    PMEM(usertable_index->map)->insert(2, 20);

    PMEM(usertable_index->map)->display();

    PMEM(db->tables)->display();

    cout << "Index 1: "
         << PMEM(PMEM(PMEM(PMEM(db->tables)->at(0))->indices)->at(0))->map
         << endl;

    sp->init = 1;
  } else {
    cout << "head : " << sp->ptrs[1] << " tail: " << sp->ptrs[2] << endl;

    db = (dbase_*) PMEM((void* ) sp->ptrs[0]);
    cout << "Database :" << OFF(db) << endl;

    PMEM(db->tables)->display();

    cout << "Index 2: "
         << PMEM(PMEM(PMEM(PMEM(db->tables)->at(0))->indices)->at(0))->map
         << endl;

    PMEM(PMEM(PMEM(PMEM(PMEM(db->tables)->at(0))->indices)->at(0))->map)->insert(
        3, 30);
    PMEM(PMEM(PMEM(PMEM(PMEM(db->tables)->at(0))->indices)->at(0))->map)->insert(
        1, 23);

    PMEM(PMEM(PMEM(PMEM(PMEM(db->tables)->at(0))->indices)->at(0))->map)
        ->display();

    tab_* usertable_3 = new tab_(14);
    pmemalloc_activate(pmp, OFF(usertable_3));
    PMEM(db->tables)->push_back(OFF(usertable_3));

    PMEM(db->tables)->display();

  }

  return 0;
}

