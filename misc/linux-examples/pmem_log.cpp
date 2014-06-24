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

void* operator new(size_t sz) throw (bad_alloc) {
  std::cerr << "::new " << std::endl;
  {
    std::lock_guard<std::mutex> lock(mutex);
    return PMEM(pmp, pmemalloc_reserve(pmp, sz));
  }
}

void operator delete(void *p) throw () {
  std::cerr << "::delete " << std::endl;
  {
    std::lock_guard<std::mutex> lock(mutex);
    pmemalloc_free(pmp, PSUB(pmp, p));
  }
}

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

  vector<T> get_data(void) {
    struct node* np = sp->head;
    vector<T> data;

    while (np) {
      data.push_back(PMEM(pmp, np)->val);
      np = PMEM(pmp, np)->next;
    }

    return data;
  }

};

class record {
 public:
  record(int _data)
      : data(_data) {
  }

  int data;
};

int main(int argc, char *argv[]) {
  const char* path = "./testfile";

  long pmp_size = 10 * 1024 * 1024;
  if ((pmp = pmemalloc_init(path, pmp_size)) == NULL)
    cout << "pmemalloc_init on :" << path << endl;

  // Test

  srand(time(NULL));
  int val, i, j;

  struct node *ptr = NULL;

  plist<record*> l(pmp);
  vector<record*> data;
  vector<record*>::iterator data_itr;

  int ops = 1;

  for (i = 0; i < ops; i++) {
    val = rand() % 10;
    record* rec = new record(val);

    pmemalloc_activate(pmp, PSUB(pmp, rec));
    l.push_back(PSUB(pmp, rec));

    data = l.get_data();
    for (data_itr = data.begin(); data_itr != data.end(); data_itr++) {
      record* tmp = (*data_itr);
      cout << PMEM(pmp, tmp)->data << " -> ";
    }
    cout << endl;

    l.erase(PSUB(pmp, rec));
    delete rec;
  }

  return 0;
}

