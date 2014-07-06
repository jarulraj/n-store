#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>

#include "ptreap.h"

using namespace std;

void lookup(ptreap *tree, unsigned int version, const unsigned long key) {
  void* ret;

  ret = tree->lookup(version, key);
  cout<< "version :: "<<version<<"  key :: "<<key <<" ";
  if (ret != NULL)
    cout << "val :: " << GPOINTER_TO_UINT(ret) << endl;
  else
    cout << "val :: not found" << endl;

}

int main(int argc, char **argv) {
  unsigned int i, *nums;
  ptreap* tree = new ptreap;

  int n = 10;
  nums = new unsigned int[n];
  for (i = 0; i < n; ++i)
    nums[i] = i;

  printf("Persistent Tree with many versions...\n");

  tree->insert(1, GUINT_TO_POINTER(nums[1]));
  tree->insert(2, GUINT_TO_POINTER(nums[2]));
  tree->insert(3, GUINT_TO_POINTER(nums[3]));

  cout << "nodes ::" << tree->nnodes << endl;

  for (i = 1; i <= 4; ++i)
     lookup(tree, tree->current_version(), i);

  // Next version
  tree->next_version();

  tree->insert(2, GUINT_TO_POINTER(nums[5]));
  tree->remove(3);

  tree->insert(4, GUINT_TO_POINTER(nums[4]));

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

  cout << "nodes ::" << tree->nnodes << endl;

  delete[] nums;

  return 0;
}
