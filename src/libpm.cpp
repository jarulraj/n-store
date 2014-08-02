// libpm (perf)

#include "libpm.h"
#include <malloc.h>

using namespace std;

void* pmp;
struct static_info* sp;
std::set<void*>* pmem_pool;
size_t pmem_size;

// Global new and delete

void* operator new(size_t sz) throw (bad_alloc) {
  void* ret = pmemalloc_reserve(sz);
  return ret;
}

void operator delete(void *p) throw () {
  pmemalloc_free(p);
}

void* pmemalloc_init(const char *path, size_t size) {
  pmem_pool = new std::set<void*>();
  pmem_size = 0;
  pmp = (void*) 1;
  return pmp;
}

void* pmemalloc_static_area() {
  void* static_area = new static_info();
  memset(static_area, 0, sizeof(static_area));
  return static_area;
}

void* pmemalloc_reserve(size_t size) {
  void* ret = calloc(1, size);
  return ret;
}

void pmemalloc_activate(void *abs_ptr_) {
  size_t len = malloc_usable_size(abs_ptr_);
  pmem_persist(abs_ptr_, len, 0);

  //cout<<"Activate :: "<<len<<endl;
  pmem_pool->insert(abs_ptr_);
  pmem_size += len;
}

void pmemalloc_free(void *abs_ptr_) {
  size_t len = malloc_usable_size(abs_ptr_);
  free(abs_ptr_);

  if (pmem_pool->count(abs_ptr_) != 0) {
    //cout<<"Free :: "<<len<<endl;
    pmem_pool->erase(abs_ptr_);
    pmem_size -= len;
  }
}

void pmemalloc_end(const char *path) {
  FILE* pmem_file = fopen(path, "w");
  if (pmem_file != NULL) {
    fprintf(pmem_file, "Active %lu \n", pmem_size);
    fclose (pmem_file);
  }
}

void pmemalloc_check(const char *path) {
  FILE* pmem_file = fopen(path, "r");
  if (pmem_file != NULL) {
    fscanf(pmem_file, "Active %lu \n", &pmem_size);
    fprintf(stdout, "Active %lu \n", pmem_size);
    fclose (pmem_file);
  }
}
