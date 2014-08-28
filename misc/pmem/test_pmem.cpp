#include <iostream>
#include <cstring>
#include <string>
#include <cassert>
#include <unistd.h>
#include <chrono>
#include <ctime>

#include "libpm.h"

using namespace std;

extern struct static_info *sp;

int main(int argc, char *argv[]) {
  const char* path = "./zfile";

  // cleanup
  unlink(path);

  long pmp_size = 1024 * 1024 * 1024;
  if ((pmp = pmemalloc_init(path, pmp_size)) == NULL)
    cout << "pmemalloc_init on :" << path << endl;

  sp = (struct static_info *) pmemalloc_static_area();

  int ops = 2 * 1024 * 1024;
  size_t sz;

  std::chrono::time_point<std::chrono::system_clock> start, end;
  std::chrono::duration<double> elapsed_seconds;
  char* vc;

  start = std::chrono::system_clock::now();
  srand(0);

  for (int i = 0; i < ops; i++) {
    sz = rand() % 512;
    vc = new char[sz];
  }

  end = std::chrono::system_clock::now();

  elapsed_seconds = end - start;
  cout << "elapsed time: " << elapsed_seconds.count() << "s\n";

  start = std::chrono::system_clock::now();
  srand(0);

  for (int i = 0; i < ops; i++) {
    sz = rand() % 1024;
    vc = (char*) malloc(sz);
  }

  end = std::chrono::system_clock::now();
  elapsed_seconds = end - start;
  cout << "elapsed time: " << elapsed_seconds.count() << "s\n";

  return 0;
}

