#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>

#include "ptreap.h"

using namespace std;

void lookup(ptreap<int, int> *tree, unsigned int version, const unsigned long key) {
  int ret;

  ret = tree->lookup(version, key);
  cout<< "version :: "<<version<<"  key :: "<<key <<" ";
  if (ret != NULL)
    cout << "val :: " << ret << endl;
  else
    cout << "val :: not found" << endl;

}

int main(int argc, char **argv) {
  unsigned int i, *nums;
  ptreap<int, int>* tree = new ptreap<int, int>();

  int n = 10;
  nums = new unsigned int[n];
  for (i = 0; i < n; ++i)
    nums[i] = i;

  printf("Persistent Tree with many versions...\n");

  tree->insert(1, 10);
  tree->insert(2, 20);
  tree->insert(3, 30);

  cout << "nodes ::" << tree->nnodes << endl;

  for (i = 1; i <= 4; ++i)
     lookup(tree, tree->current_version(), i);

  // Next version
  tree->next_version();

  tree->insert(2, 50);
  tree->remove(3);

  tree->insert(4, 40);

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
