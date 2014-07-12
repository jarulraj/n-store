#include <iostream>
#include <cassert>
#include <unistd.h>

#include "libpm.h"
#include "pbtree.h"

using namespace std;

extern struct static_info* sp;

int main() {
  const char* path = "./zfile";

  // cleanup
  unlink(path);

  long pmp_size = 10 * 1024 * 1024;
  if ((pmp = pmemalloc_init(path, pmp_size)) == NULL)
    cout << "pmemalloc_init on :" << path << endl;

  sp = (struct static_info *) pmemalloc_static_area();

  pbtree<int, int>* tree = new pbtree<int, int>(&sp->ptrs[0]);

  int key;
  int ops = 10;

  for (int i = 0; i < ops; i++) {
    key = i;
    tree->insert(key, 0);
  }

  assert(tree->size() == ops);

  tree->erase(0);
  assert(tree->size() == ops-1);

  delete tree;

}

