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
  int store_pointers;

  plist()
      : head(NULL),
        tail(NULL),
        store_pointers(0) {
  }

  plist(void** _head, void** _tail, int _store_pointers) {

    head = (struct node**) OFF(_head);
    tail = (struct node**) OFF(_tail);
    store_pointers = _store_pointers;

    cout << "head : " << head << " tail : " << tail << "\n";
  }

  friend std::ostream& operator<<(std::ostream& os, const plist& list) {
    os << "head : " << list.head << " tail : " << list.tail << "\n";
    return os;
  }

  struct node* init(T val) {
    struct node* np = NULL;
    if ((np = (struct node*) pmemalloc_reserve(pmp, sizeof(*np))) == NULL) {
      cout << "pmemalloc_reserve failed " << endl;
      return NULL;
    }

    // Link it in at the beginning of the list
    PMEM(np)->next = (*PMEM(head));
    PMEM(np)->val = val;

    pmemalloc_onactive(pmp, np, (void **) PMEM(head), np);
    pmemalloc_onactive(pmp, np, (void **) PMEM(tail), np);
    pmemalloc_activate(pmp, np);

    return np;
  }

  /*
   * If storing pointers, this will take care of persisting them as well
   */
  struct node* push_back(T val) {
    if ((*PMEM(head)) == NULL) {
      return (init(val));
    }

    struct node* np = NULL;
    struct node* tailp = NULL;

    if ((np = (struct node*) pmemalloc_reserve(pmp, sizeof(*np))) == NULL) {
      cout << "pmemalloc_reserve failed " << endl;
      return NULL;
    }

    // Link it in at the end of the list
    PMEM(np)->next = NULL;
    PMEM(np)->val = val;

    tailp = (*PMEM(tail));

    pmemalloc_onactive(pmp, np, (void **) PMEM(tail), np);
    pmemalloc_activate(pmp, np);

    PMEM(tailp)->next = np;
    pmem_persist(&PMEM(tailp)->next, sizeof(*np), 0);

    // Persists data (pointers) if needed
    if (store_pointers)
      pmemalloc_activate(pmp, val);

    return np;
  }

  T at(const int index) const {
    struct node * np = (*PMEM(head));
    unsigned int itr = 0;
    bool found = false;

    while (np != NULL) {
      if (itr == index) {
        found = true;
        break;
      } else {
        itr++;
        np = PMEM(np)->next;
      }
    }

    if (found) {
      if (store_pointers)
        return PMEM(PMEM(np)->val);
      else
        return PMEM(np)->val;
    }

    return NULL;
  }

  struct node* find(T val, struct node** prev) {
    struct node* np = (*PMEM(head));
    struct node* tmp = NULL;
    bool found = false;

    while (np != NULL) {
      if (PMEM(np)->val == val) {
        found = true;
        break;
      } else {
        tmp = np;
        np = PMEM(np)->next;
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

    if ((*PMEM(head)) == NULL) {
      return false;
    }

    np = find(val, &prev);

    if (np == NULL) {
      return -1;
    } else {
      if (prev != NULL) {
        PMEM(prev)->next = PMEM(np)->next;
        pmem_persist(&PMEM(prev)->next, sizeof(*np), 0);
      }

      // Update head and tail
      if (np == (*PMEM(head))) {
        pmemalloc_onfree(pmp, np, (void **) PMEM(head),
        PMEM(np)->next);
      } else if (np == (*tail)) {
        pmemalloc_onfree(pmp, np, (void **) PMEM(tail), prev);
      }
    }

    pmemalloc_free(pmp, np);

    return true;
  }

  void display(void) {
    struct node* np = (*PMEM(head));

    while (np) {
      cout << PMEM(np)->val << " -> ";
      np = PMEM(np)->next;
    }
    cout << endl;

  }

  void clear(void) {
    struct node* np = (*PMEM(head));
    struct node* prev = NULL;

    while (np) {
      prev = np;
      np = PMEM(np)->next;
      pmemalloc_free(pmp, prev);
    }

    (*PMEM(head)) = NULL;
    (*PMEM(tail)) = NULL;
  }

  vector<T> get_data(void) {
    struct node* np = (*PMEM(head));
    vector<T> data;

    while (np) {
      data.push_back(PMEM(np)->val);
      np = PMEM(np)->next;
    }

    return data;
  }

};

void* operator new(size_t sz) throw (bad_alloc) {
  //std::cerr << "::new " << std::endl;
  {
    std::lock_guard<std::mutex> lock(pmp_mutex);
    return PMEM(pmemalloc_reserve(pmp, sz));
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

  ptr_cnt = 0;
  dbase_* db;

  // Initialize
  if (sp->init == 0) {
    cout << "Init" << endl;
    cout << "head : " << sp->ptrs[1] << " tail: " << sp->ptrs[2] << endl;

    db = new dbase_("ycsb");
    sp->ptrs[0] = OFF(db);

    // Persist database
    pmemalloc_activate_absolute(pmp, db);

    // TABLES
    db->val = 1023;
    db->tables = OFF(new plist<tab_*>(&sp->ptrs[1], &sp->ptrs[2], true));
    pmemalloc_activate(pmp, db->tables);

    tab_* usertable = new tab_(123);
    PMEM(db->tables)->push_back(OFF(usertable));

    printf("Table Init : %s \n", PMEM(db->tables)->at(0)->get_id().c_str());

    // INDICES
    usertable->indices = OFF(
        new plist<tab_index_*>(&sp->ptrs[3], &sp->ptrs[4], true));
    pmemalloc_activate(pmp, usertable->indices);

    tab_index_* usertable_index = new tab_index_("usertable_index");
    PMEM(usertable->indices)->push_back(OFF(usertable_index));

    tab_index_* usertable_index_2 = new tab_index_("usertable_index_2");
    PMEM(usertable->indices)->push_back(OFF(usertable_index_2));

    PMEM(db->tables)->display();

    cout << "Index 1: "
         << PMEM(PMEM(db->tables)->at(0)->indices)->at(0)->get_name() << endl;

    sp->init = 1;
  } else {

    db = (dbase_*) PMEM((void* ) sp->ptrs[0]);
    cout << "Database :" << OFF(db) << endl;

    PMEM(db->tables)->display();

    cout << "Index 2: "
         << PMEM(PMEM(db->tables)->at(0)->indices)->at(1)->get_name() << endl;

  }

  return 0;
}

