#ifndef LIBPM_H_
#define LIBPM_H_

/////////////////////////////////////////////////////////////////////
// util.h -- global defines for util module
/////////////////////////////////////////////////////////////////////

#include <errno.h>
#include <stdio.h>

#define DEBUG(...)
#define ASSERT(cnd)
#define ASSERTinfo(cnd, info)
#define ASSERTeq(lhs, rhs)
#define ASSERTne(lhs, rhs)

//#define DEBUG(...)\
  debug(__FILE__, __LINE__, __func__, __VA_ARGS__)
#define FATALSYS(...)\
  fatal(errno, __FILE__, __LINE__, __func__, __VA_ARGS__)
#define FATAL(...)\
  fatal(0, __FILE__, __LINE__, __func__, __VA_ARGS__)
#define USAGE(...)\
  usage(Usage, __VA_ARGS__)

/* assert a condition is true */
//#define ASSERT(cnd)\
  ((void)((cnd) || (fatal(0, __FILE__, __LINE__, __func__,\
  "assertion failure: %s", #cnd), 0)))
/* assertion with extra info printed if assertion fails */
//#define ASSERTinfo(cnd, info) \
  ((void)((cnd) || (fatal(0, __FILE__, __LINE__, __func__,\
  "assertion failure: %s (%s = %s)", #cnd, #info, info), 0)))
/* assert two integer values are equal */
//#define ASSERTeq(lhs, rhs)\
  ((void)(((lhs) == (rhs)) || (fatal(0, __FILE__, __LINE__, __func__,\
  "assertion failure: %s (%d) == %s (%d)", #lhs,\
  (lhs), #rhs, (rhs)), 0)))
/* assert two integer values are not equal */
//#define ASSERTne(lhs, rhs)\
  ((void)(((lhs) != (rhs)) || (fatal(0, __FILE__, __LINE__, __func__,\
  "assertion failure: %s (%d) != %s (%d)", #lhs,\
  (lhs), #rhs, (rhs)), 0)))
void debug(const char *file, int line, const char *func, const char *fmt, ...);
void fatal(int err, const char *file, int line, const char *func,
           const char *fmt, ...);
void usage(const char *argfmt, const char *fmt, ...);

/////////////////////////////////////////////////////////////////////
// pmem_inline.h -- inline versions of pmem for performance
/////////////////////////////////////////////////////////////////////

#include <sys/types.h>
#include <sys/mman.h>
#include <stdio.h>
#include <stdint.h>

#define ALIGN 64  /* assumes 64B cache line size */

static inline void *
pmem_map(int fd, size_t len) {
  void *base;

  if ((base = mmap(NULL, len, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0))
      == MAP_FAILED)
    return NULL;

  return base;
}

static inline void pmem_drain_pm_stores(void) {
  /*
   * Nothing to do here -- this implementation assumes the platform
   * has something like Intel's ADR feature, which flushes HW buffers
   * automatically on power loss.  This implementation further assumes
   * the Persistent Memory hardware itself doesn't need to be alerted
   * when something needs to be persistent.  Of course these assumptions
   * may be wrong for different combinations of Persistent Memory
   * products and platforms, but this is just example code.
   *
   * TODO: update this to work on other types of platforms.
   */
}

static inline void pmem_flush_cache(void *addr, size_t len, int flags) {
  uintptr_t uptr;

  /* loop through 64B-aligned chunks covering the given range */
  for (uptr = (uintptr_t) addr & ~(ALIGN - 1); uptr < (uintptr_t) addr + len;
      uptr += 64)
    __builtin_ia32_clflush((void *) uptr);
}

static inline void pmem_persist(void *addr, size_t len, int flags) {
  pmem_flush_cache(addr, len, flags);
  __builtin_ia32_sfence();
  pmem_drain_pm_stores();
}

/////////////////////////////////////////////////////////////////////
// pmemalloc.h -- example malloc library for Persistent Memory
/////////////////////////////////////////////////////////////////////

#include <stdint.h>

/*
 * size of the static area returned by pmem_static_area()
 */
#define PMEM_STATIC_SIZE 4096

/*
 * number of onactive/onfree values allowed
 */
#define PMEM_NUM_ON 3

/*
 * given a relative pointer, add in the base associated
 * with the given Persistent Memory Pool (pmp).
 */
#define PMEM(pmp, ptr_) ((decltype(ptr_))(pmp + (uintptr_t)ptr_))
#define PSUB(pmp, ptr_) ((decltype(ptr_))((uintptr_t)ptr_ - (uintptr_t)pmp))

void *pmemalloc_init(const char *path, size_t size);
void *pmemalloc_static_area(void *pmp);
void *pmemalloc_reserve(void *pmp, size_t size);
void pmemalloc_onactive(void *pmp, void *ptr_, void **parentp_, void *nptr_);
void pmemalloc_onfree(void *pmp, void *ptr_, void **parentp_, void *nptr_);
void pmemalloc_activate(void *pmp, void *ptr_);
void pmemalloc_free(void *pmp, void *ptr_);
void pmemalloc_activate_absolute(void *pmp, void *ptr_);
void pmemalloc_free_absolute(void *pmp, void *ptr_);
void pmemalloc_check(const char *path);

#endif /* LIBPM_H_ */
