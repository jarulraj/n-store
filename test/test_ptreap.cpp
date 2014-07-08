#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>

#include "libpm.h"
#include "ptreap.h"

using namespace std;

extern struct static_info* sp;

void lookup(ptreap<int, int*> *tree, unsigned int version,
            const unsigned long key) {
  int* ret;

  ret = tree->at(key, version);
  cout << "version :: " << version << "  key :: " << key << " ";
  if (ret != NULL)
    cout << "val :: " << (*ret) << endl;
  else
    cout << "val :: not found" << endl;
}

int main(int argc, char **argv) {

  const char* path = "./testfile";

  long pmp_size = 10 * 1024 * 1024;
  if ((pmp = pmemalloc_init(path, pmp_size)) == NULL)
    cout << "pmemalloc_init on :" << path << endl;

  sp = (struct static_info *) pmemalloc_static_area();

  int i;
  ptreap<int, int*>* tree = new ptreap<int, int*>(&sp->ptrs[0]);

  int n = 10;
  int* nums = new int[n];
  for (i = 0; i < n; ++i)
    nums[i] = i * 10;

  printf("Persistent Tree with many versions...\n");

  tree->insert(1, &(nums[1]));
  tree->insert(2, &(nums[2]));
  tree->insert(3, &(nums[3]));
  tree->insert(6, &(nums[6]));

  cout << "nodes ::" << tree->nnodes << endl;

  for (i = 1; i <= 4; ++i)
    lookup(tree, tree->current_version(), i);

  // Next version
  tree->next_version();

  tree->insert(2, &(nums[5]));
  tree->remove(3);

  tree->insert(4, &(nums[4]));

  for (i = 1; i <= 4; ++i)
    lookup(tree, tree->current_version(), i);

  cout << "nodes ::" << tree->nnodes << endl;

  lookup(tree, 1, 2);
  lookup(tree, 1, 3);
  lookup(tree, 0, 2);
  lookup(tree, 0, 3);

  cout << "nodes ::" << tree->nnodes << endl;

  tree->delete_versions(0);

  lookup(tree, 0, 2);
  lookup(tree, 0, 3);

  cout << "nodes ::" << tree->nnodes << endl;

  delete tree;
  sp->ptrs[0] = NULL;

  cout << "nodes ::" << tree->nnodes << endl;

  delete[] nums;

  return 0;
}
