#include <iostream>
#include <cassert>

#include "libpm.h"
#include "ptree.h"

using namespace std;

extern struct static_info* sp;

int main() {
  const char* path = "./testfile";

  long pmp_size = 10 * 1024 * 1024;
  if ((pmp = pmemalloc_init(path, pmp_size)) == NULL)
    cout << "pmemalloc_init on :" << path << endl;

  sp = (struct static_info *) pmemalloc_static_area();

  ptree<int, int>* tree = new ptree<int, int>(&sp->ptrs[0]);

  int key;
  int ops = 10;

  for (int i = 0; i < ops; i++) {
    key = i;
    tree->insert(key, 0);
  }

  assert(tree->size == ops);

  tree->erase(0);
  assert(tree->size == ops-1);

  int ret = std::remove(path);
  assert(ret == 0);

  delete tree;

}

