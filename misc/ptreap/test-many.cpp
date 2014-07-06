#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>

#include "ptreap.h"

using namespace std;

void lookup(PTreap *tree, unsigned int version, const unsigned long key) {
  void* ret;

  ret = p_treap_lookup_v(tree, version, key);
  cout<< "version :: "<<version<<"  key :: "<<key <<" ";
  if (ret != NULL)
    cout << "val :: " << GPOINTER_TO_UINT(ret) << endl;
  else
    cout << "val :: not found" << endl;

}

int main(int argc, char **argv) {
  unsigned int i, *nums;
  PTreap *tree;

  int n = 10;
  nums = new unsigned int[n];
  for (i = 0; i < n; ++i)
    nums[i] = i;

  printf(" Persistent Tree with many versions...\n");

  tree = p_treap_new();

  p_treap_insert(tree, 1, GUINT_TO_POINTER(nums[1]));
  p_treap_insert(tree, 2, GUINT_TO_POINTER(nums[2]));
  p_treap_insert(tree, 3, GUINT_TO_POINTER(nums[3]));

  cout << "nodes ::" << tree->nnodes << endl;

  for (i = 1; i <= 4; ++i)
     lookup(tree, p_treap_current_version(tree), i);

  // Next version
  p_treap_next_version(tree);

  p_treap_insert(tree, 2, GUINT_TO_POINTER(nums[5]));
  p_treap_remove(tree, 3);

  p_treap_insert(tree, 4, GUINT_TO_POINTER(nums[4]));

  for (i = 1; i <= 4; ++i)
    lookup(tree, p_treap_current_version(tree), i);

  cout << "nodes ::" << tree->nnodes << endl;

  lookup(tree, 1, 2);
  lookup(tree, 1, 3);
  lookup(tree, 0, 2);
  lookup(tree, 0, 3);

  cout << "nodes ::" << tree->nnodes << endl;

  p_treap_delete_versions(tree, 0);

  lookup(tree, 0, 2);
  lookup(tree, 0, 3);

  cout << "nodes ::" << tree->nnodes << endl;

  p_treap_destroy(tree);

  cout << "nodes ::" << tree->nnodes << endl;

  delete[] nums;

  return 0;
}
