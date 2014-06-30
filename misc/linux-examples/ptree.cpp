#include <iostream>

#include <vector>
#include <thread>
#include <mutex>

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
  srand(time(NULL));
  int ops = 10;

  for (int i = 0; i < ops; i++) {
    key = rand() % 10;
    tree->insert(key, 0);
  }

  tree->display();

  delete tree;

}

