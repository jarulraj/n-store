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
void pmemalloc_check(const char *path);

#endif /* LIBPM_H_ */
