#include <iostream>
#include <cstdlib>
#include <cstdio>
#include <cassert>

#include <vector>
#include <thread>
#include <mutex>

#include "libpm.h"
#include <sys/time.h>

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

    if (sz % 2 == 0 || sz % 3 == 0) {
      delete prev;
      delete test;
      delete glob;
    }
  }

  return tot_sz;
}

#define CNT 1000000

size_t stress_test_pmemalloc() {
  int cnt = CNT;
  size_t tot_sz = 0;
  size_t sz;
  char* test;
  srand(0);

  for (int i = 0; i < cnt; i++) {
    if (i % 100 == 0)
      sz = rand() % 512;
    else
      sz = rand() % 32;

    tot_sz += sz;

    test = new char[sz];
  }

  return tot_sz;
}

size_t stress_test_malloc() {
  int cnt =  CNT;
  size_t tot_sz = 0;
  size_t sz;
  char* test;

  for (int i = 0; i < cnt; i++) {
    if (i % 100 == 0)
      sz = rand() % 512;
    else
      sz = rand() % 32;

    tot_sz += sz;

    test = (char*) malloc(sz);
  }

  return tot_sz;
}

int main() {
  const char* path = "./zfile";

  long pmp_size = 1024 * 1024 * 1024;
  if ((pmp = pmemalloc_init(path, pmp_size)) == NULL)
    cout << "pmemalloc_init on :" << path << endl;

  timeval t1, t2, diff;
  double duration;
  size_t alloc;

  // PMEMALLOC
  gettimeofday(&t1, NULL);
  alloc = stress_test_pmemalloc();
  gettimeofday(&t2, NULL);
  timersub(&t2, &t1, &diff);

  duration = (diff.tv_sec) * 1000.0;
  duration += (diff.tv_usec) / 1000.0;

  cout << "time :: " << duration << " (ms) " << endl;
  cout << "mem :: " << alloc << endl;


  gettimeofday(&t1, NULL);
  alloc = stress_test_malloc();
  gettimeofday(&t2, NULL);
  timersub(&t2, &t1, &diff);

  // MALLOC
  duration = (diff.tv_sec) * 1000.0;
  duration += (diff.tv_usec) / 1000.0;

  cout << "time :: " << duration << " (ms) " << endl;
  cout << "mem :: " << alloc << endl;

  return 0;
}
