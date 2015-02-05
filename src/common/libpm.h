#pragma once

#include <iostream>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <set>

#include "utils.h"

namespace storage {

#define ALIGN 64  /* assumes 64B cache line size */
#define DEBUG

/* latency in ns */
#define PCOMMIT_LATENCY 100

/* instruction set extensions */
#undef ISE

static inline void pmem_flush_cache(void *addr, size_t len,
                                    __attribute__((unused)) int flags) {
  uintptr_t uptr;

  /* loop through 64B-aligned chunks covering the given range */
  for (uptr = (uintptr_t) addr & ~(ALIGN - 1); uptr < (uintptr_t) addr + len;
      uptr += 64){
#ifdef ISE
	  // CLWB
      clwb(addr);
#else
	  // CLFLUSH
	  __builtin_ia32_clflush((void *) uptr);
#endif
  }

}

extern size_t pcommit_size;
extern size_t clflush;

static inline void pmem_persist(void *addr, size_t len, int flags) {
  pmem_flush_cache(addr, len, flags);

#ifdef ISE
  // PCOMMIT
  pcommit(PCOMMIT_LATENCY);
#else
  // SFENCE
  __builtin_ia32_sfence();
#endif

}

#define MAX_PTRS 1024

struct static_info {
  unsigned int init;
  unsigned int itr;
  void* ptrs[MAX_PTRS];
};

extern struct static_info* sp;
extern void* pmp;

void* pmemalloc_init(const char *path, size_t size);
void* pmemalloc_static_area();
void pmemalloc_activate(void *abs_ptr_);
void pmemalloc_free(void *abs_ptr_);
void pmemalloc_check(const char *path);
void pmemalloc_end(const char *path);
unsigned int get_next_pp();

}
