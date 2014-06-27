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
  plist(void** _head, void** _tail) {

    head = (struct node**) _head;
    tail = (struct node**) _tail;

    printf("sp: %p \n", PSUB(pmp, sp));
    printf("sp->head : %p \n", head);
    printf("sp->tail : %p \n", tail);
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
    pmemalloc_free(pmp, PSUB(pmp, p));
  }
}

class fd_ {
 public:
  fd_(std::string str) {

    size_t len = str.size();
    data = new char[len + 1];
    memcpy(data, str.c_str(), len + 1);

    cout << data << endl;
  }

  ~fd_() {
    delete data;
  }

  char* data;
};

class rec_ {
 public:
  rec_(int _val) {
    vec[0] = _val;
    vec[1] = _val + 1;
  }

  std::string get_val() {
    return std::to_string(vec[0]);
  }

  int vec[2];
};

int main(int argc, char *argv[]) {
  const char* path = "./testfile";

  long pmp_size = 10 * 1024 * 1024;
  if ((pmp = pmemalloc_init(path, pmp_size)) == NULL)
    cout << "pmemalloc_init on :" << path << endl;

  sp = (struct static_info *) pmemalloc_static_area(pmp);

  // Test

  srand(time(NULL));
  int val, i, j;

  // LIST 1

  ptr_cnt = 0;
  cout << "count: " << ptr_cnt << endl;

  plist<rec_*> l(&(sp->ptrs[ptr_cnt++]), &(sp->ptrs[ptr_cnt++]));
  vector<rec_*> data;
  vector<rec_*>::iterator data_itr;

  val = rand() % 10;
  std::string val_str = std::to_string(val);

  rec_* r = new rec_(val);
  pmemalloc_activate(pmp, PSUB(pmp, r));

  l.push_back(PSUB(pmp, r));

  data = l.get_data();
  for (data_itr = data.begin(); data_itr != data.end(); data_itr++) {
    rec_* tmp = (*data_itr);
    cout << "rec* : " << tmp << " " << PMEM(pmp, tmp)->vec[0] << "  "
         << PMEM(pmp, tmp)->vec[1] << endl;
  }

  //l.erase(PSUB(pmp, r));
  //delete r;

  // LIST 2

  plist<int> m(&(sp->ptrs[ptr_cnt++]), &(sp->ptrs[ptr_cnt++]));

  val = rand() % 10;
  m.push_back(val);

  m.display();

  return 0;
}

