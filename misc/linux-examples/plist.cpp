#include <iostream>
#include <cstring>
#include <string>

#include <vector>
#include <thread>
#include <mutex>

#include "libpm.h"
#include "plist.h"

using namespace std;

void* pmp;
std::mutex pmp_mutex;

#define MAX_PTRS 128
struct static_info {
  int init;
  void* ptrs[MAX_PTRS];
};
struct static_info *sp;

void* operator new(size_t sz) throw (bad_alloc) {
  std::lock_guard<std::mutex> lock(pmp_mutex);
  return PMEM(pmemalloc_reserve(pmp, sz));
}

void operator delete(void *p) throw () {
  std::lock_guard<std::mutex> lock(pmp_mutex);
  pmemalloc_free_absolute(pmp, p);
}

int main(int argc, char *argv[]) {
  const char* path = "./testfile";

  long pmp_size = 10 * 1024 * 1024;
  if ((pmp = pmemalloc_init(path, pmp_size)) == NULL)
    cout << "pmemalloc_init on :" << path << endl;

  sp = (struct static_info *) pmemalloc_static_area(pmp);

  plist<char*>* list = new plist<char*>(&sp->ptrs[0], &sp->ptrs[1]);

  int key;
  srand(time(NULL));
  int ops = 3;

  for (int i = 0; i < ops; i++) {
    key = rand() % 10;

    std::string str(2, 'a' + key);
    char* data = new char[3];
    pmemalloc_activate_absolute(pmp, data);
    strcpy(data, str.c_str());

    list->push_back(OFF(data));
  }

  list->display();

  char* updated_val = new char[3];
  pmemalloc_activate_absolute(pmp, updated_val);
  strcpy(updated_val, "ab");

  list->update(2, OFF(updated_val));

  updated_val = new char[3];
  pmemalloc_activate_absolute(pmp, updated_val);
  strcpy(updated_val, "cd");

  list->update(0, OFF(updated_val));

  list->display();

  delete list;

  return 0;
}

