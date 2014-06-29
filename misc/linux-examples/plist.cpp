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
  int ops = 10;

  for (int i = 0; i < ops; i++) {
    key = rand() % 10;

    std::string str(10, 'a'+key);
    char* data = new char[10];
    pmemalloc_activate_absolute(pmp, data);

    strcpy(data, str.c_str());
    list->push_back(OFF(data));
  }

  vector<char*> vec = list->get_data();
  vector<char*>::iterator vec_itr;

  for(vec_itr = vec.begin() ; vec_itr != vec.end() ; vec_itr++){
    char* ptr = (*vec_itr);
    cout<<" data :"<<PMEM(ptr)<<endl;
  }


  //delete list;

  return 0;
}

