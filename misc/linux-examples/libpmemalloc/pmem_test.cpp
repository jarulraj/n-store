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

struct node {
  struct node *next_;
  int value;
};

struct node* parent_;
struct node* np_;

struct static_info {
  struct node *rootnp_;
};

#define MY_POOL_SIZE  (1024 * 1024)

class list {
 public:
  list(void* _pmp)
      : pmp(_pmp) {

    sp = (struct static_info *) pmemalloc_static_area(pmp);

  }

  void push_back(struct node) {
    if ((np_ = pmemalloc_reserve(pmp, sizeof(*np_))) == NULL)
      FATALSYS("pmemalloc_reserve");

    /* link it in at the beginning of the list */
    PMEM(pmp, np_)->next_ = sp->rootnp_;
    PMEM(pmp, np_)->value = value;
    pmemalloc_onactive(pmp, np_, (void **) &sp->rootnp_, np_);
    pmemalloc_activate(pmp, np_);

  }

  struct static_info *sp;
  void* pmp;
};

int main(int argc, char *argv[]) {
  std::string path;
  void* pmp;

  path = "./testfile";

  if ((pmp = pmemalloc_init(path.c_str(), MY_POOL_SIZE)) == NULL)
    FATALSYS("pmemalloc_init on %s", path.c_str());

  /*
   * remove first item from list
   */
  if (sp->rootnp_ == NULL)
    FATAL("the list is empty");

  np_ = sp->rootnp_;
  pmemalloc_onfree(pmp, np_, (void **) &sp->rootnp_,
  PMEM(pmp, np_)->next_);
  pmemalloc_free(pmp, np_);

  char *sep = "";

  /*
   * traverse the list, printing the numbers found
   */
  np_ = sp->rootnp_;
  while (np_) {
    printf("%s%d", sep, PMEM(pmp, np_)->value);
    sep = " ";
    np_ = PMEM(pmp, np_)->next_;
  }
  printf("\n");
}

return 0;
}

