// libpm (perf)

#include "libpm.h"
#include <malloc.h>

using namespace std;

void* pmp;
struct static_info* sp;
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
  pmem_size = 0;
  pmp = (void*) 1;
  return pmp;
}

void* pmemalloc_static_area() {
  void* static_area = new static_info();
  memset(static_area, 0 , sizeof(static_area));
  return static_area;
}

void* pmemalloc_reserve(size_t size) {
  // Use calloc instead of malloc
  void* ret = calloc(1, size);
  pmem_size += size;
  return ret;
}

void pmemalloc_activate(void *abs_ptr_) {
  size_t len = malloc_usable_size(abs_ptr_);
  //printf("persist :: %p %lu \n", abs_ptr_, len);
  pmem_persist(abs_ptr_, len, 0);
}

void pmemalloc_free(void *abs_ptr_) {
  size_t len = malloc_usable_size(abs_ptr_);
  pmem_size -= len;
  free(abs_ptr_);
}

void pmemalloc_check(const char *path) {
  printf("pmem_size :: %lu \n", pmem_size);
}
