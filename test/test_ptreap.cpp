#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>

#include "libpm.h"
#include "ptreap.h"

namespace storage {

extern struct static_info* sp;

void ptreap_lookup(ptreap<int, int*> *tree, unsigned int version,
                        const unsigned long key) {
  int* ret;

  ret = tree->at(key, version);
  //std::cout << "version :: " << version << "  key :: " << key << " ";
  if (ret != NULL) {
    //std::cout << "val :: " << (*ret) << std::endl;
  } else {
    //std::cout << "val :: not found" << std::endl;
  }
}

void test_ptreap() {
  const char* path = "./zfile";

  // cleanup
  unlink(path);

  long pmp_size = 10 * 1024 * 1024;
  if ((pmp = pmemalloc_init(path, pmp_size)) == NULL)
    std::cout << "pmemalloc_init on :" << path << std::endl;

  sp = (struct static_info *) pmemalloc_static_area();

  int i;
  ptreap<int, int*>* tree = new ptreap<int, int*>(&sp->ptrs[0]);

  int n = 10;
  int* nums = new int[n];
  for (i = 0; i < n; ++i)
    nums[i] = i * 10;

  //printf("Persistent Tree with many versions...\n");

  tree->insert(1, &(nums[1]));
  tree->insert(2, &(nums[2]));
  tree->insert(3, &(nums[3]));
  tree->insert(6, &(nums[6]));

  //std::cout << "nodes ::" << tree->nnodes << std::endl;

  for (i = 1; i <= 4; ++i)
    ptreap_lookup(tree, tree->current_version(), i);

  // Next version
  tree->next_version();

  tree->insert(2, &(nums[5]));
  tree->remove(3);

  tree->insert(4, &(nums[4]));

  for (i = 1; i <= 4; ++i)
    ptreap_lookup(tree, tree->current_version(), i);

  //std::cout << "nodes ::" << tree->nnodes << std::endl;

  ptreap_lookup(tree, 1, 2);
  ptreap_lookup(tree, 1, 3);
  ptreap_lookup(tree, 0, 2);
  ptreap_lookup(tree, 0, 3);

  //std::cout << "nodes ::" << tree->nnodes << std::endl;

  tree->delete_versions(0);

  ptreap_lookup(tree, 0, 2);
  ptreap_lookup(tree, 0, 3);

  //std::cout << "nodes ::" << tree->nnodes << std::endl;

  delete tree;
  sp->ptrs[0] = NULL;

  //std::cout << "nodes ::" << tree->nnodes << std::endl;

  delete[] nums;

}

}

int main(int argc, char **argv) {
  storage::test_ptreap();
  return 0;
}
