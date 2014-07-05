#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>

#include "ptree.h"

using namespace std;

static int cmp(const void* k1, const void* k2) {
  long r = GPOINTER_TO_UINT(k1) - GPOINTER_TO_UINT(k2);
  return r < 0 ? -1 : (r == 0 ? 0 : 1);
}

static void time_normalize(struct timeval *len) {
  while (len->tv_usec < 0) {
    --len->tv_sec;
    len->tv_usec += 1000000;
  }
  while (len->tv_usec >= 1000000) {
    ++len->tv_sec;
    len->tv_usec -= 1000000;
  }
}

static void time_distance(struct timeval start, struct timeval end,
                          struct timeval *len) {
  len->tv_sec = end.tv_sec - start.tv_sec;
  len->tv_usec = end.tv_usec - start.tv_usec;

  time_normalize(len);
}

int main(int argc, char **argv) {
  struct timeval start, end, len;
  unsigned int i, n, seed, *nums;
  PTree *tree;

  if (argc != 3)
    return 1;

  seed = atol(argv[1]);
  n = atol(argv[2]);

  nums = new unsigned int[n];
  for (i = 0; i < n; ++i)
    nums[i] = i;

  printf(" Persistent Tree with many versions...\n");

  tree = p_tree_new(cmp);

  cout<< "tree :: "<< tree <<endl;

  printf("  Inserting %8d elements...", n);
  fflush(stdout);
  gettimeofday(&start, NULL);
  for (i = 0; i < n; ++i) {
    cout<<" node ::"<<tree->nnodes <<endl;
    p_tree_next_version(tree);
    p_tree_insert(tree, GUINT_TO_POINTER(nums[i]), GUINT_TO_POINTER(nums[i]));
  }

  p_tree_next_version(tree);
  gettimeofday(&end, NULL);
  time_distance(start, end, &len);
  printf("done. %lu.%06lu seconds", (unsigned long) len.tv_sec,
         (unsigned long) len.tv_usec);
  printf(" %u rotations\n", p_tree_rotations());

  printf("  Performing %8d queries...", n);
  fflush(stdout);
  gettimeofday(&start, NULL);
  for (i = 0; i < n ; ++i){
    void* ret = p_tree_lookup(tree, GUINT_TO_POINTER(nums[i]+1));
    cout<<"ret :: "<<GPOINTER_TO_UINT(ret)<<endl;
  }
  gettimeofday(&end, NULL);
  time_distance(start, end, &len);
  printf("done. %lu.%06lu seconds", (unsigned long) len.tv_sec,
         (unsigned long) len.tv_usec);
  printf(" %u rotations\n", p_tree_rotations());

  printf("  Deleting  %8d elements...", n);
  fflush(stdout);
  gettimeofday(&start, NULL);
  for (i = 0; i < n ; ++i)
    p_tree_remove(tree, GUINT_TO_POINTER(nums[i]));
  gettimeofday(&end, NULL);
  time_distance(start, end, &len);
  printf("done. %lu.%06lu seconds", (unsigned long) len.tv_sec,
         (unsigned long) len.tv_usec);
  printf(" %u rotations\n", p_tree_rotations());

  cout<<" nodes ::"<<tree->nnodes <<endl;

  printf("  Destroying the tree...        ");
  fflush(stdout);
  gettimeofday(&start, NULL);
  p_tree_unref(tree);
  gettimeofday(&end, NULL);
  time_distance(start, end, &len);
  printf("done. %lu.%06lu seconds", (unsigned long) len.tv_sec,
         (unsigned long) len.tv_usec);
  printf(" %u rotations\n", p_tree_rotations());

  delete[] nums;

  return 0;
}
