#include <iostream>
#include <cstring>
#include <string>
#include <cassert>
#include <unistd.h>

#include "libpm.h"
#include "plist.h"

namespace storage {

void test_pmem() {
  const char* path = "./zfile";

  // cleanup
  unlink(path);

  long pmp_size = 1024 * 1024 * 1024;
  if ((pmp = pmemalloc_init(path, pmp_size)) == NULL)
    std::cout << "pmemalloc_init on :" << path << std::endl;

  sp = (struct static_info *) pmemalloc_static_area();

  int ops = 1024;
  size_t sz;

  for (int i = 0; i < ops; i++) {
    sz = rand() % 1024;

    char* vc = new char[sz];
    if (sz % 4 == 0)
      pmemalloc_activate(vc);

    if (sz % 16 == 0)
      delete vc;
  }
}

}

extern struct static_info *sp;

int main(int argc, char *argv[]) {
  storage::test_pmem();
  return 0;
}

