#ifndef LIBPM_H_
#define LIBPM_H_

#include <iostream>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <set>

using namespace std;

#define ALIGN 64  /* assumes 64B cache line size */
#define DEBUG

static inline void pmem_flush_cache(void *addr, size_t len, __attribute__((unused)) int flags) {
  uintptr_t uptr;

  /* loop through 64B-aligned chunks covering the given range */
  for (uptr = (uintptr_t) addr & ~(ALIGN - 1); uptr < (uintptr_t) addr + len;
      uptr += 64)
    __builtin_ia32_clflush((void *) uptr);
}

static inline void pmem_persist(void *addr, size_t len, int flags) {
  pmem_flush_cache(addr, len, flags);
  __builtin_ia32_sfence();
  //pmem_drain_pm_stores();
}

#define MAX_PTRS 512
struct static_info {
  unsigned int init;
  unsigned int itr;
  void* ptrs[MAX_PTRS];
};

extern struct static_info* sp;
extern void* pmp;
extern bool pm_stats;

void* pmemalloc_init(const char *path, size_t size);
void* pmemalloc_static_area();
void* pmemalloc_reserve(size_t size);
void pmemalloc_activate(void *abs_ptr_);
void pmemalloc_free(void *abs_ptr_);
void pmemalloc_check(const char *path);
void pmemalloc_end(const char *path);
void pmemalloc_count(void *abs_ptr_);

#endif /* LIBPM_H_ */
