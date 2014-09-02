#include <iostream>
#include <cstring>
#include <string>
#include <cassert>
#include <unistd.h>
#include <chrono>
#include <ctime>
#include <thread>
#include <vector>

#include "libpm.h"

using namespace std;

std::string fs_path = "./";
size_t pmp_size = 64 * 1024 * 1024;

void do_task(unsigned int tid) {

  std::cout << "tid :: " << tid << std::endl;

  storage::pmem_pool pp = storage::pmem_pool(fs_path, tid, pmp_size);

  std::vector<void*> ptrs;

  int ops = 1024 * 1024 * 4;
  int ptrs_offset = 0;
  size_t sz;

  std::chrono::time_point<std::chrono::system_clock> start, end;
  std::chrono::duration<double> elapsed_seconds;
  char* vc;

  start = std::chrono::system_clock::now();
  srand(0);

  for (int i = 0; i < ops; i++) {
    sz = 1 + rand() % 32;
    vc = (char*) pp.pmemalloc_reserve(sz);

    pp.pmemalloc_activate(vc);

    ptrs.push_back(vc);
    ptrs_offset = rand() % ptrs.size();

    if (rand() % 1024 != 0 && ptrs.size() >= 3) {
      pp.pmemalloc_free(ptrs[ptrs_offset]);
      ptrs.erase(ptrs.begin() + ptrs_offset);
    }
  }

  end = std::chrono::system_clock::now();

  elapsed_seconds = end - start;
  std::cout << "elapsed time: " << elapsed_seconds.count() << "s\n";
  cout.flush();

  ptrs.clear();

  start = std::chrono::system_clock::now();
  srand(0);

  for (int i = 0; i < ops; i++) {
    sz = 1 + rand() % 32;
    vc = (char*) malloc(sz);

    ptrs.push_back(vc);
    ptrs_offset = rand() % ptrs.size();

    if (rand() % 1024 != 0 && ptrs.size() >= 3) {
      free(ptrs[ptrs_offset]);
      ptrs.erase(ptrs.begin() + ptrs_offset);
    }
  }

  end = std::chrono::system_clock::now();
  elapsed_seconds = end - start;
  cout << "elapsed time: " << elapsed_seconds.count() << "s\n";
}

int main(int argc, char *argv[]) {
  std::vector<void*> ptrs;

  std::vector<std::thread> executors;
  unsigned int num_executors = 2;

  for (unsigned int i = 0; i < num_executors; i++)
    executors.push_back(std::thread(&do_task, i));

  for (unsigned int i = 0; i < num_executors; i++)
    executors[i].join();

  return 0;
}

