#include <iostream>
#include <cstring>
#include <string>
#include <cassert>
#include <unistd.h>
#include <chrono>
#include <ctime>

#include <vector>

#include "libpm.h"

using namespace std;

int main(int argc, char *argv[]) {

  const char* path = "./zfile";

  std::vector<void*> ptrs;

  // cleanup
  unlink(path);

  long pmp_size = 64 * 1024 * 1024;
  if ((pmp = pmemalloc_init(path, pmp_size)) == NULL)
    cout << "pmemalloc_init on :" << path << endl;

  sp = (struct static_info *) pmemalloc_static_area();

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
    vc = (char*) pmemalloc_reserve(sz);

    ptrs.push_back(vc);
    ptrs_offset = rand() % ptrs.size();

    if (rand() % 1024 != 0 && ptrs.size() >= 3) {
      pmemalloc_free(ptrs[ptrs_offset]);
      ptrs.erase(ptrs.begin() + ptrs_offset);
    }
  }

  end = std::chrono::system_clock::now();

  elapsed_seconds = end - start;
  cout << "elapsed time: " << elapsed_seconds.count() << "s\n";

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

  return 0;
}

