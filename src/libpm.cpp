// libpm (perf)

#include "libpm.h"
#include <malloc.h>
#include <mutex>

using namespace std;

void* pmp;
struct static_info* sp;
std::set<void*>* pmem_pool;
std::recursive_mutex pmem_mutex;
size_t pmem_size;
bool pm_stats = false;

// Global new and delete

void* operator new(size_t sz) throw (bad_alloc) {
  void* ret = calloc(1, sz);
  return ret;
}

void operator delete(void *p) throw () {
  if (pm_stats) {
    pmem_mutex.lock();
    if (pmem_pool->count(p) != 0) {
      size_t len = malloc_usable_size(p);
      pmem_pool->erase(p);
      pmem_size -= len;
    }
    pmem_mutex.unlock();
  }

  free(p);
}

void *operator new[](std::size_t sz) throw (std::bad_alloc) {
  void* ret = calloc(1, sz);
  return ret;
}

void operator delete[](void *p) throw () {
  if (pm_stats) {
    pmem_mutex.lock();
    if (pmem_pool->count(p) != 0) {
      size_t len = malloc_usable_size(p);
      pmem_pool->erase(p);
      pmem_size -= len;
    }
    pmem_mutex.unlock();
  }

  free(p);
}

void* pmemalloc_reserve(size_t size) {
  void* ret = malloc(size);
  return ret;
}

void pmemalloc_free(void *abs_ptr_) {
  if (pm_stats) {
    pmem_mutex.lock();
    if (pmem_pool->count(abs_ptr_) != 0) {
      size_t len = malloc_usable_size(abs_ptr_);
      pmem_pool->erase(abs_ptr_);
      pmem_size -= len;
    }
    pmem_mutex.unlock();
  }

  free(abs_ptr_);
}

void* pmemalloc_init(__attribute__((unused)) const char *path,
                     __attribute__((unused))      size_t size) {
  pmem_pool = new std::set<void*>();
  pmem_size = 0;
  pmp = (void*) 1;
  return pmp;
}

void* pmemalloc_static_area() {
  void* static_area = new static_info();
  return static_area;
}

void pmemalloc_activate(void *abs_ptr_) {
  size_t len = malloc_usable_size(abs_ptr_);
  pmem_persist(abs_ptr_, len, 0);

  if (pm_stats) {
    pmem_mutex.lock();
    pmem_pool->insert(abs_ptr_);
    pmem_mutex.unlock();

    pmem_size += len;
  }
}

void pmemalloc_count(void *abs_ptr_) {
  if (pm_stats) {
    size_t len = malloc_usable_size(abs_ptr_);

    pmem_mutex.lock();
    pmem_pool->insert(abs_ptr_);
    pmem_mutex.unlock();

    pmem_size += len;
  }
}

void pmemalloc_end(const char *path) {
  FILE* pmem_file = fopen(path, "w");
  if (pmem_file != NULL) {
    fprintf(pmem_file, "Active %lu \n", pmem_size);
    fclose(pmem_file);
  }
}

void pmemalloc_check(const char *path) {
  FILE* pmem_file = fopen(path, "r");
  if (pmem_file != NULL) {
    int ret = fscanf(pmem_file, "Active %lu \n", &pmem_size);
    if (ret >= 0) {
      fprintf(stdout, "Active %lu \n", pmem_size);
    } else {
      perror("fscanf");
    }
    fclose(pmem_file);
  }
}
