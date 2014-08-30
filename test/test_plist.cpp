#include <iostream>
#include <cstring>
#include <string>
#include <cassert>
#include <unistd.h>

#include "libpm.h"
#include "plist.h"

namespace storage {

int test_plist() {
  const char* path = "./zfile";

// cleanup
  unlink(path);

  long pmp_size = 10 * 1024 * 1024;
  if ((pmp = pmemalloc_init(path, pmp_size)) == NULL)
    std::cout << "pmemalloc_init on :" << path << std::endl;

  sp = (struct static_info *) pmemalloc_static_area();

  plist<char*>* list = new plist<char*>(&sp->ptrs[0], &sp->ptrs[1]);

  int key;
  srand(time(NULL));
  int ops = 3;

  for (int i = 0; i < ops; i++) {
    key = rand() % 10;

    std::string str(2, 'a' + key);
    char* data = new char[3];
    pmemalloc_activate(data);
    strcpy(data, str.c_str());

    list->push_back(data);
  }

  char* updated_val = new char[3];
  pmemalloc_activate(updated_val);
  strcpy(updated_val, "ab");

  list->update(2, updated_val);
  list->display();

  assert(list->at(2) == updated_val);

  updated_val = new char[3];
  pmemalloc_activate(updated_val);
  strcpy(updated_val, "cd");

  list->update(0, updated_val);

  assert(list->at(0) == updated_val);

  delete list;

  int ret = std::remove(path);

  return ret;
}

}

extern struct static_info *sp;

int main(int argc, char *argv[]) {
  storage::test_plist();

  return 0;
}

