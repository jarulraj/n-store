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

extern "C" {
#include "util/util.h"
#include "libpmem/pmem.h"
#include "libpmemalloc/pmemalloc.h"
}

using namespace std;

template<typename T>
class plist {
 private:
  struct node {
    struct node* next;
    T val;
  };

  struct static_info {
    struct node* head;
    struct node* tail;
  };

  struct static_info *sp;
  void* pmp;

 public:
  plist(void* _pmp)
      : pmp(_pmp) {

    sp = (struct static_info *) pmemalloc_static_area(pmp);

  }

  struct node* init(T val) {
    //cout << "creating list with headnode : " << val << endl;

    struct node* np = NULL;
    if ((np = (struct node*) pmemalloc_reserve(pmp, sizeof(*np))) == NULL) {
      cout << "pmemalloc_reserve failed " << endl;
      return NULL;
    }

    // link it in at the beginning of the list
    PMEM(pmp, np)->next = sp->head;
    PMEM(pmp, np)->val = val;

    pmemalloc_onactive(pmp, np, (void **) &sp->head, np);
    pmemalloc_onactive(pmp, np, (void **) &sp->tail, np);
    pmemalloc_activate(pmp, np);

    return np;
  }

  struct node* push_back(T val) {
    if (sp->head == NULL) {
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

    tailp = sp->tail;

    pmemalloc_onactive(pmp, np, (void **) &sp->tail, np);
    pmemalloc_activate(pmp, np);

    // persist already active pointer
    PMEM(pmp, tailp)->next = np;
    pmem_persist(&PMEM(pmp, tailp)->next, sizeof(*np), 0);

    return np;
  }

  struct node* find(T val, struct node** prev) {
    struct node* np = sp->head;
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

    if (sp->head == NULL) {
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
      if (np == sp->head) {
        pmemalloc_onfree(pmp, np, (void **) &sp->head, PMEM(pmp, np)->next);
      } else if (np == sp->tail) {
        pmemalloc_onfree(pmp, np, (void **) &sp->tail, prev);
      }
    }

    pmemalloc_free(pmp, np);

    return true;
  }

  void display(void) {
    struct node* np = sp->head;

    while (np) {
      cout << PMEM(pmp, np)->val << " -> ";
      np = PMEM(pmp, np)->next;
    }
    cout << endl;

  }

};

int main(int argc, char *argv[]) {
  std::string path;
  void* pmp;

  path = "./testfile";

  // Global Persistent Memory Pool
  long pmp_size = 10 * 1024 * 1024;

  if ((pmp = pmemalloc_init(path.c_str(), pmp_size)) == NULL)
    cout << "pmemalloc_init on :" << path.c_str() << endl;

  srand(time(NULL));
  int ins = rand() % 10;
  int del = rand() % 10;
  int val, i, j;

  struct node *ptr = NULL;

  plist<int> l(pmp);

  for (i = 0; i < ins; i++) {
    val = rand() % 10;
    l.push_back(val);
  }

  l.display();

  for (i = 0; i < del; i++) {
    val = rand() % 10;
    l.erase(val);
  }

  l.display();

  return 0;
}

