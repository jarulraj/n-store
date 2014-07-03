#include <iostream>
#include <cstdlib>
#include <cstdio>
#include <cassert>

#include <vector>
#include <thread>
#include <mutex>

#include "libpm.h"

using namespace std;

pthread_mutex_t pmp_mutex = PTHREAD_MUTEX_INITIALIZER;

int new_cnt = 0;

void* operator new(size_t sz) throw (bad_alloc) {
  new_cnt++;
  pthread_mutex_lock(&pmp_mutex);
  void* ret = pmemalloc_reserve(sz);
  pthread_mutex_unlock(&pmp_mutex);
  return ret;
}

void operator delete(void *p) throw () {
  pthread_mutex_lock(&pmp_mutex);
  pmemalloc_free(p);
  pthread_mutex_unlock(&pmp_mutex);
}

void work() {
  int cnt = 16 * 1024;

  for (int i = 0; i < cnt; i++) {

    int* test1 = new int(10);
    pmemalloc_activate(test1);

    vector<int> intvec;

    delete test1;
  }

}

int main() {
  const char* path = "./testfile";

  long pmp_size = 10 * 1024 * 1024;
  if ((pmp = pmemalloc_init(path, pmp_size)) == NULL)
    cout << "pmemalloc_init on :" << path << endl;

  int num_thds = 2;
  std::vector<std::thread> executors;

  for (int i = 0; i < num_thds; i++)
    executors.push_back(std::thread(work));

  for (int i = 0; i < num_thds; i++)
    executors[i].join();

  cout << "new_cnt :" << new_cnt << endl;

  return 0;
}
