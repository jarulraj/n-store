#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <string>

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <stdarg.h>

#include <vector>
#include <thread>
#include <mutex>

#include "libpm.h"

using namespace std;

void* pmp;
std::mutex pmp_mutex;

#define MAX_PTRS 128
int ptr_cnt = 0;

struct static_info {
  int init;
  void* ptrs[MAX_PTRS];
};

struct static_info *sp;

template<typename T>
class plist {
 public:
  struct node {
    struct node* next;
    T val;
  };

  struct node** head;
  struct node** tail;

  plist()
      : head(NULL),
        tail(NULL) {
  }

  plist(void** _head, void** _tail) {

    head = (struct node**) PSUB(pmp, _head);
    tail = (struct node**) PSUB(pmp, _tail);

    printf("sp: %p \n", PSUB(pmp, sp));
    printf("sp->head : %p \n", PSUB(pmp, head));
    printf("sp->tail : %p \n", PSUB(pmp, tail));
  }

  friend std::ostream& operator<<(std::ostream& os, const plist& list) {
    os << "head : " << list.head << " tail : " << list.tail << "\n";
    return os;
  }

  struct node* init(T val) {
    //cout << "creating list with headnode : " << val << endl;

    struct node* np = NULL;
    if ((np = (struct node*) pmemalloc_reserve(pmp, sizeof(*np))) == NULL) {
      cout << "pmemalloc_reserve failed " << endl;
      return NULL;
    }

    // link it in at the beginning of the list
    PMEM(pmp, np)->next = (*PMEM(pmp, head));
    PMEM(pmp, np)->val = val;

    pmemalloc_onactive(pmp, np, (void **) PMEM(pmp, head), np);
    pmemalloc_onactive(pmp, np, (void **) PMEM(pmp, tail), np);
    pmemalloc_activate(pmp, np);

    return np;
  }

  struct node* push_back(T val) {
    if ((*PMEM(pmp, head)) == NULL) {
      return (init(val));
    }

    struct node* np = NULL;
    struct node* tailp = NULL;

    if ((np = (struct node*) pmemalloc_reserve(pmp, sizeof(*np))) == NULL) {
      cout << "pmemalloc_reserve failed " << endl;
      return NULL;
    }

    // link it in at the end of the list
    PMEM(pmp, np)->next = NULL;
    PMEM(pmp, np)->val = val;

    tailp = (*PMEM(pmp, tail));

    pmemalloc_onactive(pmp, np, (void **) PMEM(pmp, tail), np);
    pmemalloc_activate(pmp, np);

    // persist already active pointer
    PMEM(pmp, tailp)->next = np;
    pmem_persist(&PMEM(pmp, tailp)->next, sizeof(*np), 0);

    return np;
  }

  struct node* at(unsigned int id) {
    struct node* np = (*PMEM(pmp, head));
    unsigned int itr = 0;

    while (np != NULL) {
      if (itr == id)
        return np;
      else
        np = PMEM(pmp, np)->next;
    }

    return NULL;
  }

  struct node* find(T val, struct node** prev) {
    struct node* np = (*PMEM(pmp, head));
    struct node* tmp = NULL;

    bool found = false;

    while (np != NULL) {
      if (PMEM(pmp, np)->val == val) {
        found = true;
        break;
      } else {
        tmp = np;
        np = PMEM(pmp, np)->next;
      }
    }

    if (found) {
      if (prev) {
        *prev = tmp;
      }
      return np;
    } else {
      return NULL;
    }
  }

  bool erase(T val) {
    struct node* prev = NULL;
    struct node* np = NULL;

    if ((*PMEM(pmp, head)) == NULL) {
      return false;
    }

    np = find(val, &prev);

    if (np == NULL) {
      return -1;
    } else {
      if (prev != NULL) {
        PMEM(pmp, prev)->next = PMEM(pmp, np)->next;
        pmem_persist(&PMEM(pmp, prev)->next, sizeof(*np), 0);
      }

      // Update head and tail
      if (np == (*PMEM(pmp, head))) {
        pmemalloc_onfree(pmp, np, (void **) PMEM(pmp, head),
                         PMEM(pmp, np)->next);
      } else if (np == (*tail)) {
        pmemalloc_onfree(pmp, np, (void **) PMEM(pmp, tail), prev);
      }
    }

    pmemalloc_free(pmp, np);

    return true;
  }

  void display(void) {
    struct node* np = (*PMEM(pmp, head));

    while (np) {
      cout << PMEM(pmp, np)->val << " -> ";
      np = PMEM(pmp, np)->next;
    }
    cout << endl;

  }

  void clear(void) {
    struct node* np = (*PMEM(pmp, head));
    struct node* prev = NULL;

    while (np) {
      prev = np;
      np = PMEM(pmp, np)->next;
      pmemalloc_free(pmp, prev);
    }

    (*PMEM(pmp, head)) = NULL;
    (*PMEM(pmp, tail)) = NULL;
  }

  vector<T> get_data(void) {
    struct node* np = (*PMEM(pmp, head));
    vector<T> data;

    while (np) {
      data.push_back(PMEM(pmp, np)->val);
      np = PMEM(pmp, np)->next;
    }

    return data;
  }

};

void* operator new(size_t sz) throw (bad_alloc) {
  //std::cerr << "::new " << std::endl;
  {
    std::lock_guard<std::mutex> lock(pmp_mutex);
    return PMEM(pmp, pmemalloc_reserve(pmp, sz));
  }
}

void operator delete(void *p) throw () {
  //std::cerr << "::delete " << std::endl;
  {
    std::lock_guard<std::mutex> lock(pmp_mutex);
    pmemalloc_free_absolute(pmp, p);
  }
}

class tab_index_ {
 public:
  tab_index_(std::string str) {

    size_t len = str.size();
    data = PSUB(pmp, new char[len + 1]);

    memcpy(PMEM(pmp, data), str.c_str(), len + 1);
    pmemalloc_activate(pmp, data);

  }

  std::string get_name() {
    return std::string(PMEM(pmp, data));
  }

  ~tab_index_() {
    delete PMEM(pmp, data);
  }

  char* data;
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
    data = PSUB(pmp, new char[len + 1]);

    memcpy(PMEM(pmp, data), str.c_str(), len + 1);
    pmemalloc_activate(pmp, data);

    val = 2;
  }

  ~dbase_() {
    tables->clear();

    delete PMEM(pmp, data);
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

  ptr_cnt = 0;
  dbase_* db;

  // Initialize
  if (sp->init == 0) {
    cout << "Init" << endl;
    cout << "head : " << sp->ptrs[1] << " tail: " << sp->ptrs[2] << endl;

    db = new dbase_("ycsb");
    sp->ptrs[0] = PSUB(pmp, db);

    // Persist database
    pmemalloc_activate_absolute(pmp, db);

    // TABLES
    db->val = 1023;
    db->tables = PSUB(pmp, new plist<tab_*>(&(sp->ptrs[1]), &(sp->ptrs[2])));

    pmemalloc_activate(pmp, db->tables);

    tab_* usertable = new tab_(123);
    // Persist table
    pmemalloc_activate_absolute(pmp, usertable);

    PMEM(pmp, db->tables)->push_back(PSUB(pmp, usertable));

    printf("Table Init : %s \n",
    PMEM(pmp, PMEM(pmp, PMEM(pmp, db->tables)->at(0))->val)->get_id().c_str());

    // INDICES
    usertable->indices = PSUB(pmp, new plist<tab_index_*>(&(sp->ptrs[3]), &(sp->ptrs[4])));

    pmemalloc_activate(pmp, usertable->indices);

    tab_index_* usertable_index = new tab_index_("usertable_index");
    // Persist table index
    pmemalloc_activate_absolute(pmp, usertable_index);

    PMEM(pmp, usertable->indices)->push_back(PSUB(pmp, usertable_index));

    cout << "Tables :" << db->tables << endl;

    PMEM(pmp, db->tables)->display();
    cout << "head : " << PMEM(pmp, db->tables)->head << " tail: "
         << PMEM(pmp, db->tables)->tail << endl;

    cout<< "index head: "<< PMEM(pmp, usertable->indices)->head << " "
        << PMEM(pmp, usertable->indices)->tail << endl;

    sp->init = 1;
  } else {

    db = (dbase_*) PMEM(pmp, (void* ) sp->ptrs[0]);
    cout << "Database :" << db << endl;

    cout << "Tables :" << db->tables << endl;
    cout << "head : " << PMEM(pmp, db->tables)->head << " tail: "
         << PMEM(pmp, db->tables)->tail << endl;

    PMEM(pmp, db->tables)->display();

    printf("Table : %s \n",
    PMEM(pmp, PMEM(pmp, PMEM(pmp, db->tables)->at(0))->val)->get_id().c_str());

    cout<< "Table index : "<< PMEM(pmp, PMEM(pmp, PMEM(pmp, PMEM(pmp, PMEM(pmp, PMEM(pmp, db->tables)->at(0))->val)->indices)->at(0))->val)->get_name() << endl;

  }

  return 0;
}

