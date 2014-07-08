#include <iostream>
#include <cstring>
#include <string>
#include <cassert>
#include <unistd.h>

#include "libpm.h"
#include "plist.h"

using namespace std;

extern struct static_info *sp;

int main(int argc, char *argv[]) {
  const char* path = "./zfile";

  long pmp_size = 1024 * 1024 * 1024;
  if ((pmp = pmemalloc_init(path, pmp_size)) == NULL)
    cout << "pmemalloc_init on :" << path << endl;

  sp = (struct static_info *) pmemalloc_static_area();

  int ops = 1024;
  size_t sz;

  for(int i = 0 ; i < ops ; i++){
    sz = rand() % 1024;

    char* vc = new char[sz];
    if(sz % 4 == 0)
      pmemalloc_activate(vc);

    if(sz % 16 == 0)
      delete vc;
  }

  // cleanup
  unlink(path);

  return 0;
}

