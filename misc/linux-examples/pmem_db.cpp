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

#define MAX_PTRS 512
int ptr_cnt = 0;

struct static_info {
  void* ptrs[MAX_PTRS];
};

struct static_info *sp;

template<typename T>
class plist {
 private:
  struct node {
    struct node* next;
    T val;
  };

  struct node** head;
  struct node** tail;

 public:
  plist()
      : head(NULL),
        tail(NULL) {
  }

  plist(void** _head, void** _tail) {

    head = (struct node**) _head;
    tail = (struct node**) _tail;

    printf("sp: %p \n", PSUB(pmp, sp));
    printf("sp->head : %p \n", PSUB(pmp, head));
    printf("sp->tail : %p \n", PSUB(pmp, tail));
  }

  struct node* init(T val) {
    //cout << "creating list with headnode : " << val << endl;

    struct node* np = NULL;
    if ((np = (struct node*) pmemalloc_reserve(pmp, sizeof(*np))) == NULL) {
      cout << "pmemalloc_reserve failed " << endl;
      return NULL;
    }

    // link it in at the beginning of the list
    PMEM(pmp, np)->next = (*head);
    PMEM(pmp, np)->val = val;

    pmemalloc_onactive(pmp, np, (void **) head, np);
    pmemalloc_onactive(pmp, np, (void **) tail, np);
    pmemalloc_activate(pmp, np);

    return np;
  }

  struct node* push_back(T val) {
    if ((*head) == NULL) {
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

    tailp = (*tail);

    pmemalloc_onactive(pmp, np, (void **) tail, np);
    pmemalloc_activate(pmp, np);

    // persist already active pointer
    PMEM(pmp, tailp)->next = np;
    pmem_persist(&PMEM(pmp, tailp)->next, sizeof(*np), 0);

    return np;
  }

  struct node* at(unsigned int id) {
    struct node* np = (*head);
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
    struct node* np = (*head);
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

    if ((*head) == NULL) {
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
      if (np == (*head)) {
        pmemalloc_onfree(pmp, np, (void **) head, PMEM(pmp, np)->next);
      } else if (np == (*tail)) {
        pmemalloc_onfree(pmp, np, (void **) tail, prev);
      }
    }

    pmemalloc_free(pmp, np);

    return true;
  }

  void display(void) {
    struct node* np = (*head);

    while (np) {
      cout << PMEM(pmp, np)->val << " -> ";
      np = PMEM(pmp, np)->next;
    }
    cout << endl;

  }

  vector<T> get_data(void) {
    struct node* np = (*head);
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

class tab_ {
 public:
  tab_(int _val)
      : table_id(_val) {
  }

  std::string get_id() {
    return std::to_string(table_id);
  }

  int table_id;
};

class dbase_ {
 public:
  dbase_(std::string str) {

    size_t len = str.size();
    data = PSUB(pmp, new char[len + 1]);

    memcpy(PMEM(pmp, data), str.c_str(), len + 1);
    pmemalloc_activate(pmp, data);

    val = 2;
  }

  ~dbase_() {
    delete data;
  }

  int val;
  char* data;
  plist<tab_*> tables;
};

int main(int argc, char *argv[]) {
  const char* path = "./testfile";

  long pmp_size = 10 * 1024 * 1024;
  if ((pmp = pmemalloc_init(path, pmp_size)) == NULL)
    cout << "pmemalloc_init on :" << path << endl;

  sp = (struct static_info *) pmemalloc_static_area(pmp);

  ptr_cnt = 0;
  dbase_* db;

  if (sp->ptrs[0] == NULL) {
    db = new dbase_("ycsb");
    sp->ptrs[0] = PSUB(pmp, db);

    // Persist database
    pmemalloc_activate_absolute(pmp, db);
  } else {
    cout << "database :" << sp->ptrs[0] << endl;
    db = (dbase_*) PMEM(pmp, (void* ) sp->ptrs[0]);
  }

  printf("database : id :: %d name :: %s \n", db->val, PMEM(pmp, db->data));

  db->tables = plist<tab_*>(&(sp->ptrs[1]), &(sp->ptrs[2]));

  srand(time(NULL));
  int val = rand() % 10;

  tab_* usertable = new tab_(val);

  // Persist table
  pmemalloc_activate_absolute(pmp, usertable);

  db->tables.push_back(PSUB(pmp, usertable));
  db->tables.display();

  cout << "table :" << PMEM(pmp, PMEM(pmp, db->tables.at(0))->val)->get_id() << endl;

  return 0;
}

