#include <iostream>
#include <cstdlib>
#include <cstdio>
#include <cassert>

#include <vector>
#include <thread>
#include <mutex>

#include "libpm.h"

using namespace std;

size_t work() {
  int cnt = 16 * 8;
  size_t tot_sz = 0;
  size_t sz;
  char* prev = NULL;
  char* test = NULL;
  char* glob = NULL;

  for (int i = 0; i < cnt; i++) {
    sz = rand() % (1024 * 32);
    tot_sz += sz;
    //printf("sz :: %lu tot_sz :: %lu \n", sz, tot_sz);

    prev = test;
    test = new char[sz];
    pmemalloc_activate(test);

    if (sz % 2 ==0 || sz % 3 == 0){
      delete prev;
      delete test;
      delete glob;
    }
  }

  return tot_sz;
}

int main() {
  const char* path = "./zfile";

  long pmp_size = 1024 * 1024;
  if ((pmp = pmemalloc_init(path, pmp_size)) == NULL)
    cout << "pmemalloc_init on :" << path << endl;

  size_t alloc = work();
  cout << "mem :: " << alloc << endl;

  return 0;
}
