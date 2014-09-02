#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <stdint.h>
#include <string.h>
#include <stdarg.h>
#include <fcntl.h>
#include <dirent.h>
#include <pthread.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/param.h>

#include <string>

namespace storage {

// Logging

// size of the static area returned by pmem_static_area()
#define PMEM_STATIC_SIZE 4096
#define MAX_PTRS 512

#define ABS_PTR(p, pmp) ((decltype(p))(pmp + (uintptr_t)p))
#define REL_PTR(p, pmp) ((decltype(p))((uintptr_t)p - (uintptr_t)pmp))

/* 64B cache line size */
#define ALIGN 64

struct static_info {
  unsigned int init;
  unsigned int itr;
  void* ptrs[MAX_PTRS];
};

struct clump {
  size_t size;  // size of the clump
  size_t prevsize;
};

class pmem_pool {
 public:
  pmem_pool()
      : pmp(NULL),
        sp(NULL),
        location(0),
        pool_size(0),
        prev_clp(NULL) {
  }

  pmem_pool(std::string fs_path, unsigned int pid, size_t _pool_size);

  void *pmemalloc_reserve(size_t size);
  void pmemalloc_activate(void *abs_ptr_);
  void pmemalloc_free(void *abs_ptr_);

  void pmemalloc_check(const char *path);
  unsigned int pmemalloc_next_pp();

  void *pmem_map(int fd, size_t len);
  void *pmemalloc_init(std::string path, size_t size);
  void *pmemalloc_static_area();
  void pmemalloc_display();
  void pmemalloc_check();
  void pmemalloc_validate(struct clump* clp);

  // memory pool
  void* pmp;
  struct static_info* sp;
  void* location;
  size_t pool_size;
  struct clump* prev_clp;
};

}
