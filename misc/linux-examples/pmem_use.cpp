#include <iostream>
#include <cstdlib>
#include <cstdio>
#include <cassert>

#include <vector>
#include <thread>
#include <mutex>

#include "libpm.h"

using namespace std;

void* pmp;
std::mutex pmp_mutex;

void* operator new(size_t sz) throw (bad_alloc) {
  std::cerr << "::new" << std::endl;
  {
    std::lock_guard<std::mutex> lock(mutex);
    return PMEM(pmp, pmemalloc_reserve(pmp, sz));
  }
}

void operator delete(void *p) throw () {
  std::cerr << "::delete" << std::endl;
  {
    std::lock_guard<std::mutex> lock(mutex);
    pmemalloc_free(pmp, PSUB(pmp, p));
  }
}

void work() {

  for (int i = 0; i < 16; i++) {

    int* test1 = new int(10);
    int* test2 = new int[5];
    vector<int> intvec;
    vector<int>::iterator it;

    intvec.insert(it, 10);

    delete test1;
    delete[] test2;
  }

}

int main() {
  const char* path = "./testfile";

  long pmp_size = 10 * 1024 * 1024;
  if ((pmp = pmemalloc_init(path, pmp_size)) == NULL)
    cout << "pmemalloc_init on :" << path << endl;

  int num_thds = 4;
  std::vector<std::thread> executors;

  for (int i = 0; i < num_thds; i++)
    executors.push_back(std::thread(work));

  for (int i = 0; i < num_thds; i++)
    executors[i].join();

  return 0;
}
