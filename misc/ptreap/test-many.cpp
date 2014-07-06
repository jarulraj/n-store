#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>

#include "ptree.h"

using namespace std;

void lookup(PTree *tree, unsigned int version, const unsigned long key) {
  void* ret;

  ret = p_tree_lookup_v(tree, version, key);
  cout<< "version :: "<<version<<"  key :: "<<key <<" ";
  if (ret != NULL)
    cout << "val :: " << GPOINTER_TO_UINT(ret) << endl;
  else
    cout << "val :: not found" << endl;

}

int main(int argc, char **argv) {
  unsigned int i, *nums;
  PTree *tree;

  int n = 10;
  nums = new unsigned int[n];
  for (i = 0; i < n; ++i)
    nums[i] = i;

  printf(" Persistent Tree with many versions...\n");

  tree = p_tree_new();
  p_tree_next_version(tree);
  p_tree_next_version(tree);

  p_tree_insert(tree, 1, GUINT_TO_POINTER(nums[1]));
  p_tree_insert(tree, 2, GUINT_TO_POINTER(nums[2]));
  p_tree_insert(tree, 3, GUINT_TO_POINTER(nums[3]));

  cout << "nodes ::" << tree->nnodes << endl;

  for (i = 1; i <= 4; ++i)
     lookup(tree, p_tree_current_version(tree), i);

  // Next version
  p_tree_next_version(tree);

  p_tree_insert(tree, 2, GUINT_TO_POINTER(nums[5]));
  p_tree_remove(tree, 3);

  p_tree_insert(tree, 4, GUINT_TO_POINTER(nums[4]));

  for (i = 1; i <= 4; ++i)
    lookup(tree, p_tree_current_version(tree), i);

  cout << "nodes ::" << tree->nnodes << endl;

  lookup(tree, 3, 3);
  lookup(tree, 3, 2);
  lookup(tree, 2, 3);
  lookup(tree, 2, 2);

  //p_tree_delete_versions(tree, p_tree_current_version(tree) - 1);

  cout << "nodes ::" << tree->nnodes << endl;

  /*
   printf("  Deleting  %8d elements... \n", n);
   for (i = 0; i < n; ++i)
   p_tree_remove(tree, i);
   */

  delete[] nums;

  return 0;
}
