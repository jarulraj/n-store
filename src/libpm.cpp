// libpm

#include "libpm.h"

#define roundup2(x, y)  (((x)+((y)-1))&(~((y)-1))) /* if y is powers of two */
#define rounddown2(x, y) ((x)&(~((y)-1)))          /* if y is power of two */
namespace storage {

#define DEBUG(...)
#define ASSERT(cnd)
#define ASSERTinfo(cnd, info)
#define ASSERTeq(lhs, rhs)
#define ASSERTne(lhs, rhs)

#define FATALSYS(...)\
  fatal(errno, __FILE__, __LINE__, __func__, __VA_ARGS__)
#define FATAL(...)\
  fatal(0, __FILE__, __LINE__, __func__, __VA_ARGS__)
#define USAGE(...)\
  usage(Usage, __VA_ARGS__)

/*
 #define DEBUG(...)\
  debug(__FILE__, __LINE__, __func__, __VA_ARGS__)
 // assert a condition is true
 #define ASSERT(cnd)\
  ((void)((cnd) || (fatal(0, __FILE__, __LINE__, __func__,\
  "assertion failure: %s", #cnd), 0)))
 // assertion with extra info printed if assertion fails
 #define ASSERTinfo(cnd, info) \
  ((void)((cnd) || (fatal(0, __FILE__, __LINE__, __func__,\
  "assertion failure: %s (%s = %s)", #cnd, #info, info), 0)))
 // assert two integer values are equal
 #define ASSERTeq(lhs, rhs)\
  ((void)(((lhs) == (rhs)) || (fatal(0, __FILE__, __LINE__, __func__,\
  "assertion failure: %s (%d) == %s (%d)", #lhs,\
  (lhs), #rhs, (rhs)), 0)))
 // assert two integer values are not equal
 #define ASSERTne(lhs, rhs)\
  ((void)(((lhs) != (rhs)) || (fatal(0, __FILE__, __LINE__, __func__,\
  "assertion failure: %s (%d) != %s (%d)", #lhs,\
  (lhs), #rhs, (rhs)), 0)))
 */

// debug -- printf-like debug messages
void debug(const char *file, int line, const char *func, const char *fmt, ...);

// fatal -- printf-like error exits, with and without errno printing
void fatal(int err, const char *file, int line, const char *func,
           const char *fmt, ...);

// pool header kept at a known location in each memory-mapped file
struct pool_header {
  char signature[16]; /* must be PMEM_SIGNATURE */
  size_t totalsize; /* total file size */
  char padding[4096 - 16 - sizeof(size_t)];
};

// definitions used internally by this implementation
#define PMEM_SIGNATURE "*PMEMALLOC_POOL"
#define PMEM_PAGE_SIZE 4096 /* size of next three sections */
#define PMEM_STATIC_OFFSET 4096 /* offset of static area */
#define PMEM_HDR_OFFSET 12288 /* offset of pool header */

#define PMEM_CLUMP_OFFSET 16384 /* offset of first clump */
#define PMEM_MIN_POOL_SIZE (1024 * 1024)

#define PMEM_CHUNK_SIZE 16 /* alignment/granularity for all allocations */
#define PMEM_STATE_MASK 15 /* for storing state in size lower bits */

#define PMEM_STATE_FREE 0 /* free clump */
#define PMEM_STATE_RESERVED 1 /* reserved clump */
#define PMEM_STATE_ACTIVE 2 /* active clump */
#define PMEM_STATE_UNUSED 3 /* must be highest value + 1 */

void* pmem_pool::pmem_map(int fd, size_t len) {
  void *base;

  if ((base = mmap(location, len, PROT_READ | PROT_WRITE,
  MAP_SHARED | MAP_POPULATE,
                   fd, 0)) == MAP_FAILED)
    return NULL;

  return base;
}

static inline void pmem_flush_cache(void *addr, size_t len,
                                    __attribute((unused)) int flags) {
  uintptr_t uptr = (uintptr_t) addr & ~(ALIGN - 1);
  uintptr_t end = (uintptr_t) addr + len;

  /* loop through 64B-aligned chunks covering the given range */
  for (; uptr < end; uptr += ALIGN)
    __builtin_ia32_clflush((void *) uptr);
}

static inline void pmem_persist(void *addr, size_t len, int flags) {
  pmem_flush_cache(addr, len, flags);
  __builtin_ia32_sfence();
}

// pmemalloc_recover -- recover after a possible crash
static void pmemalloc_recover(void* pmp) {
  struct clump *clp;

  DEBUG("pmp=0x%lx", pmp);

  clp = ABS_PTR((struct clump *) PMEM_CLUMP_OFFSET, pmp);

  while (clp->size) {
    size_t sz = clp->size & ~PMEM_STATE_MASK;
    int state = clp->size & PMEM_STATE_MASK;
    DEBUG("[0x%lx]clump size %lu state %d", REL_PTR(clp, pmp), sz, state);

    switch (state) {
      case PMEM_STATE_RESERVED:
        /* return the clump to the FREE pool */
        clp->size = sz | PMEM_STATE_FREE;
        pmem_persist(clp, sizeof(*clp), 0);
        break;
    }

    clp = (struct clump *) ((uintptr_t) clp + sz);
    DEBUG("next clp %lx, offset 0x%lx", clp, REL_PTR(clp, pmp));
  }
}

// pmemalloc_coalesce -- find adjacent free blocks and coalesce across pool
static void pmemalloc_coalesce(void* pmp) {
  struct clump *clp;
  struct clump *firstfree;
  struct clump *lastfree;
  size_t csize;

  DEBUG("pmp=0x%lx", pmp);

  firstfree = lastfree = NULL;
  csize = 0;
  clp = ABS_PTR((struct clump *) PMEM_CLUMP_OFFSET, pmp);

  while (clp->size) {
    size_t sz = clp->size & ~PMEM_STATE_MASK;
    int state = clp->size & PMEM_STATE_MASK;

    DEBUG("[0x%lx]clump size %lu state %d", REL_PTR(clp, pmp), sz, state);

    if (state == PMEM_STATE_FREE) {
      if (firstfree == NULL)
        firstfree = clp;
      else
        lastfree = clp;
      csize += sz;
    } else if (firstfree != NULL && lastfree != NULL) {
      firstfree->size = csize | PMEM_STATE_FREE;
      pmem_persist(firstfree, sizeof(*firstfree), 0);
      firstfree = lastfree = NULL;
      csize = 0;
    } else {
      firstfree = lastfree = NULL;
      csize = 0;
    }

    clp = (struct clump *) ((uintptr_t) clp + sz);
    DEBUG("next clp %lx, offset 0x%lx", clp, REL_PTR(clp, pmp));
  }

  if (firstfree != NULL && lastfree != NULL) {
    firstfree->size = csize | PMEM_STATE_FREE;
    pmem_persist(firstfree, sizeof(*firstfree), 0);
  }

}

// pmemalloc_init -- setup a Persistent Memory pool for use
void *pmem_pool::pmemalloc_init(std::string path, size_t size) {
  void *pmp;
  int err;
  int fd = -1;
  struct stat stbuf;

  DEBUG("path=%s size=0x%lx", path.c_str(), size);

  if (stat(path.c_str(), &stbuf) < 0) {
    struct clump cl = clump();
    struct pool_header hdr = pool_header();
    size_t lastclumpoff;

    if (errno != ENOENT)
      goto out;

    /*
     * file didn't exist, we're creating a new memory pool
     */
    if (size < PMEM_MIN_POOL_SIZE) {
      DEBUG("size %lu too small (must be at least %lu)", size,
          PMEM_MIN_POOL_SIZE);
      errno = EINVAL;
      goto out;
    }

    ASSERTeq(sizeof(cl), PMEM_CHUNK_SIZE); ASSERTeq(sizeof(hdr), PMEM_PAGE_SIZE);

    if ((fd = open(path.c_str(), O_CREAT | O_RDWR, 0666)) < 0)
      goto out;

    if ((errno = posix_fallocate(fd, 0, size)) != 0)
      goto out;

    /*
     * location of last clump is calculated by rounding the file
     * size down to a multiple of 64, and then subtracting off
     * another 64 to hold the struct clump.  the last clump is
     * indicated by a size of zero (so no write is necessary
     * here since the file is initially zeros.
     */
    lastclumpoff = (size & ~(PMEM_CHUNK_SIZE - 1)) - PMEM_CHUNK_SIZE;

    /*
     * create the first clump to cover the entire pool
     */
    cl.size = lastclumpoff - PMEM_CLUMP_OFFSET;
    if (pwrite(fd, &cl, sizeof(cl), PMEM_CLUMP_OFFSET) < 0)
      goto out; DEBUG("[0x%lx] created clump, size 0x%lx", PMEM_CLUMP_OFFSET, cl.size);

    /*
     * write the pool header
     */
    strcpy(hdr.signature, PMEM_SIGNATURE);
    hdr.totalsize = size;
    if (pwrite(fd, &hdr, sizeof(hdr), PMEM_HDR_OFFSET) < 0)
      goto out;

    if (fsync(fd) < 0)
      goto out;

  } else {

    if ((fd = open(path.c_str(), O_RDWR)) < 0)
      goto out;

    size = stbuf.st_size;
  }

  /*
   * map the file
   */
  if ((pmp = pmem_map(fd, size)) == NULL) {
    DEBUG("fd : %d size : %lu \n", fd, size);
    perror("mapping failed ");
    goto out;
  }

  /*
   * scan pool for recovery work, five kinds:
   *  1. pmem pool file sisn't even fully setup
   *  2. RESERVED clumps that need to be freed
   *  3. ACTIVATING clumps that need to be ACTIVE
   *  4. FREEING clumps that need to be freed
   *  5. adjacent free clumps that need to be coalesced
   */
  pmemalloc_recover(pmp);
  pmemalloc_coalesce(pmp);

  return pmp;

  out: err = errno;
  if (fd != -1)
    close(fd);
  errno = err;
  return NULL;
}

// pmemalloc_static_area -- return a pointer to the static 4k area
void *pmem_pool::pmemalloc_static_area() {
  DEBUG("pmp=0x%lx", pmp);

  return ABS_PTR((void *) PMEM_STATIC_OFFSET, pmp);
}

#define LIBPM 268435456 //0x10000000

pmem_pool::pmem_pool(std::string fs_path, unsigned int pid, size_t _pool_size)
    : pool_size(_pool_size) {

  void* addr = (void*) LIBPM;
  for (unsigned int ptr_itr = 0; ptr_itr < pid; ptr_itr++)
    addr += LIBPM;

  std::string path = fs_path + std::to_string(pid) + ".pmem";

  location = addr;
  DEBUG("location : %p", location);

  if ((pmp = pmemalloc_init(path, pool_size)) == NULL) {
    perror("pmemalloc_init");
    exit(EXIT_FAILURE);
  }

  sp = (static_info *) pmemalloc_static_area();
  prev_clp = NULL;
}

unsigned int pmem_pool::pmemalloc_next_pp() {
  unsigned int ret = sp->itr;
  sp->itr++;
  return ret;
}

// ROTATING FIRST FIT

// pmemalloc_reserve -- allocate memory, volatile until pmemalloc_activate()
void* pmem_pool::pmemalloc_reserve(size_t size) {
  size_t nsize;

  nsize = roundup2(size, PMEM_CHUNK_SIZE);
  DEBUG("size= %zu", nsize);

  struct clump *clp;
  struct clump* next_clp;
  bool loop = false;

  if (prev_clp != NULL) {
    clp = prev_clp;
  } else {
    clp = ABS_PTR((struct clump *) PMEM_CLUMP_OFFSET, pmp);
  }

  DEBUG("clp= %p", clp);

  /* first fit */
  check:
  //unsigned int itr = 0;
  while (clp->size) {
    DEBUG("************** itr :: %lu ", itr++);
    size_t sz = clp->size & ~PMEM_STATE_MASK;
    int state = clp->size & PMEM_STATE_MASK;
    DEBUG("size : %lu state : %d", sz, state);

    if (nsize <= sz) {
      if (state == PMEM_STATE_FREE) {
        void *ptr = (void *) (uintptr_t) clp + PMEM_CHUNK_SIZE
            - (uintptr_t) pmp;
        size_t leftover = sz - nsize;

        DEBUG("fit found ptr 0x%lx, leftover %lu bytes", ptr, leftover);
        if (leftover >= PMEM_CHUNK_SIZE * 2) {
          struct clump *newclp;
          newclp = (struct clump *) ((uintptr_t) clp + nsize);

          DEBUG("splitting: [0x%lx] new clump", REL_PTR(newclp, pmp));
          /*
           * can go ahead and start fiddling with
           * this freely since it is in the middle
           * of a free clump until we change fields
           * in *clp.  order here is important:
           *  1. initialize new clump
           *  2. persist new clump
           *  3. initialize existing clump do list
           *  4. persist existing clump
           *  5. set new clump size, RESERVED
           *  6. persist existing clump
           */
          newclp->size = leftover | PMEM_STATE_FREE;
          newclp->prevsize = nsize;
          pmem_persist(newclp, sizeof(*newclp), 0);

          next_clp = (struct clump *) ((uintptr_t) newclp + leftover);
          next_clp->prevsize = leftover;
          pmem_persist(next_clp, sizeof(*next_clp), 0);

          clp->size = nsize | PMEM_STATE_RESERVED;
          pmem_persist(clp, sizeof(*clp), 0);

          DEBUG("validate new clump %p", REL_PTR(newclp, pmp));
          //pmemalloc_validate(newclp);
          DEBUG("validate orig clump %p", REL_PTR(clp, pmp));
          //pmemalloc_validate(clp);
          DEBUG("validate next clump %p", REL_PTR(next_clp, pmp));
          //pmemalloc_validate(next_clp);
        } else {
          DEBUG("no split required");

          clp->size = sz | PMEM_STATE_RESERVED;
          pmem_persist(clp, sizeof(*clp), 0);

          next_clp = (struct clump *) ((uintptr_t) clp + sz);
          next_clp->prevsize = sz;
          pmem_persist(next_clp, sizeof(*next_clp), 0);

          DEBUG("validate orig clump %p", REL_PTR(clp, pmp));
          //pmemalloc_validate(clp);
          DEBUG("validate next clump %p", REL_PTR(next_clp, pmp));
          //pmemalloc_validate(next_clp);
        }

        //pmemalloc_display();

        prev_clp = clp;
        return ABS_PTR(ptr, pmp);
      }
    }

    clp = (struct clump *) ((uintptr_t) clp + sz);
    DEBUG("next clump :: [0x%lx]", REL_PTR(clp, pmp));
  }

  if (loop == false) {
    DEBUG("LOOP ");
    loop = true;
    clp = ABS_PTR((struct clump *) PMEM_CLUMP_OFFSET, pmp);
    goto check;
  }

  printf("no free memory of size %lu available \n", nsize);
  //display();
  errno = ENOMEM;
  exit(EXIT_FAILURE);
  return NULL;
}

// pmemalloc_activate -- atomically persist memory, mark in-use, store pointers
void pmem_pool::pmemalloc_activate(void *abs_ptr) {
  struct clump *clp;
  size_t sz;
  DEBUG("ptr_=%lx", abs_ptr);

  clp = (struct clump *) ((uintptr_t) abs_ptr - PMEM_CHUNK_SIZE);

  ASSERTeq(clp->size & PMEM_STATE_MASK, PMEM_STATE_RESERVED);
  sz = clp->size & ~PMEM_STATE_MASK;

  pmem_persist(abs_ptr, clp->size - PMEM_CHUNK_SIZE, 0);

  clp->size = sz | PMEM_STATE_ACTIVE;
  pmem_persist(clp, sizeof(*clp), 0);
}

// pmemalloc_free -- free memory, find adjacent free blocks and coalesce them
void pmem_pool::pmemalloc_free(void *abs_ptr_) {
  if (abs_ptr_ == NULL)
    return;

  struct clump *clp, *firstfree, *lastfree, *next_clp;
  bool first = true, last = true;
  size_t csize;
  size_t sz;

  firstfree = lastfree = NULL;
  csize = 0;

  DEBUG("ptr_=%lx", abs_ptr_);

  clp = (struct clump *) ((uintptr_t) abs_ptr_ - PMEM_CHUNK_SIZE);
  sz = clp->size & ~PMEM_STATE_MASK;
  DEBUG("size=%lu", sz);

  lastfree = (struct clump *) ((uintptr_t) clp + sz);
  DEBUG("validate lastfree %p", REL_PTR(lastfree, pmp));
  //pmemalloc_validate(lastfree);

  if ((lastfree->size & PMEM_STATE_MASK) != PMEM_STATE_FREE)
    last = false;

  firstfree = (struct clump *) ((uintptr_t) clp - clp->prevsize);
  DEBUG("validate firstfree %p", REL_PTR(firstfree, pmp));
  //pmemalloc_validate(firstfree);

  if (firstfree == clp
      || ((firstfree->size & PMEM_STATE_MASK) != PMEM_STATE_FREE))
    first = false;

  if (first && last) {
    DEBUG("******* F C L ");

    size_t first_sz = firstfree->size & ~PMEM_STATE_MASK;
    size_t last_sz = lastfree->size & ~PMEM_STATE_MASK;
    csize = first_sz + sz + last_sz;
    firstfree->size = csize | PMEM_STATE_FREE;
    pmem_persist(firstfree, sizeof(*firstfree), 0);

    next_clp = (struct clump *) ((uintptr_t) lastfree + last_sz);
    next_clp->prevsize = csize;
    pmem_persist(next_clp, sizeof(*next_clp), 0);

    prev_clp = firstfree;

    DEBUG("validate firstfree %p", REL_PTR(firstfree, pmp));
    //pmemalloc_validate(firstfree);
  } else if (first) {
    DEBUG("******* F C  ");

    size_t first_sz = firstfree->size & ~PMEM_STATE_MASK;
    csize = first_sz + sz;
    firstfree->size = csize | PMEM_STATE_FREE;
    pmem_persist(firstfree, sizeof(*firstfree), 0);

    next_clp = lastfree;
    next_clp->prevsize = csize;
    pmem_persist(next_clp, sizeof(*next_clp), 0);

    prev_clp = firstfree;

    DEBUG("validate firstfree %p", REL_PTR(firstfree, pmp));
    //pmemalloc_validate(firstfree);
    DEBUG("validate lastfree %p", REL_PTR(lastfree, pmp));
    //pmemalloc_validate(lastfree);
  } else if (last) {
    DEBUG("******* C L ");
    size_t last_sz = lastfree->size & ~PMEM_STATE_MASK;

    csize = sz + last_sz;
    clp->size = csize | PMEM_STATE_FREE;
    pmem_persist(clp, sizeof(*clp), 0);

    next_clp = (struct clump *) ((uintptr_t) lastfree + last_sz);
    next_clp->prevsize = csize;
    pmem_persist(next_clp, sizeof(*next_clp), 0);

    prev_clp = clp;

    DEBUG("validate firstfree %p", REL_PTR(firstfree, pmp));
    //pmemalloc_validate(firstfree);
    DEBUG("validate clump %p", REL_PTR(clp, pmp));
    //pmemalloc_validate(clp);
  } else {
    DEBUG("******* C ");

    csize = sz;
    clp->size = csize | PMEM_STATE_FREE;
    pmem_persist(clp, sizeof(*clp), 0);

    DEBUG("validate clump %p", REL_PTR(clp, pmp));
    //pmemalloc_validate(clp);
  }

  //pmemalloc_display();

}

//  pmemalloc_check -- check the consistency of a pmem pool
void pmem_pool::pmemalloc_check(const char *path) {
  void *pmp;
  int fd;
  struct stat stbuf;
  struct clump *clp;
  struct clump *lastclp;
  struct pool_header *hdrp;
  size_t clumptotal;
  /*
   * stats we keep for each type of memory:
   *  stats[PMEM_STATE_FREE] for free clumps
   *  stats[PMEM_STATE_RESERVED] for reserved clumps
   *  stats[PMEM_STATE_ACTIVE] for active clumps
   *  stats[PMEM_STATE_UNUSED] for overall totals
   */
  struct pmem_stat {
    size_t largest;
    size_t smallest;
    size_t bytes;
    unsigned count;
  } stats[PMEM_STATE_UNUSED + 1];

  for (unsigned int p_itr = 0; p_itr < (PMEM_STATE_UNUSED + 1); p_itr++)
    stats[p_itr] = pmem_stat();

  const char *names[] = { "Free", "Reserved", "Active", "TOTAL", };
  int i;

  DEBUG("path=%s", path);

  if ((fd = open(path, O_RDONLY)) < 0)
    FATALSYS("%s", path);

  if (fstat(fd, &stbuf) < 0)
    FATALSYS("fstat");

  DEBUG("file size 0x%lx", stbuf.st_size);

  if (stbuf.st_size < PMEM_MIN_POOL_SIZE)
    FATAL("size %lu too small (must be at least %lu)", stbuf.st_size,
          PMEM_MIN_POOL_SIZE);

  location = (void*) LIBPM;
  if ((pmp = mmap(location, stbuf.st_size, PROT_READ,
  MAP_SHARED | MAP_POPULATE,
                  fd, 0)) == MAP_FAILED)
    FATALSYS("mmap"); DEBUG("pmp %lx", pmp);

  close(fd);

  hdrp = ABS_PTR((struct pool_header *) PMEM_HDR_OFFSET, pmp);
  DEBUG("   hdrp 0x%lx (off 0x%lx)", hdrp, REL_PTR(hdrp, pmp));

  if (strcmp(hdrp->signature, PMEM_SIGNATURE))
    FATAL("failed signature check"); DEBUG("signature check passed");

  clp = ABS_PTR((struct clump *) PMEM_CLUMP_OFFSET, pmp);
  /*
   * location of last clump is calculated by rounding the file
   * size down to a multiple of 16, and then subtracting off
   * another 16 to hold the struct clump.  the last clump is
   * indicated by a size of zero.
   */
  lastclp =
      ABS_PTR(
          ((struct clump *) rounddown2(stbuf.st_size, PMEM_CHUNK_SIZE)) - PMEM_CHUNK_SIZE,
          pmp);
  DEBUG("clp 0x%lx (off 0x%lx)", clp, REL_PTR(clp, pmp)); DEBUG("lastclp 0x%lx (off 0x%lx)", lastclp, REL_PTR(lastclp, pmp));

  clumptotal = (uintptr_t) lastclp - (uintptr_t) clp;
  DEBUG("expected clumptotal: %lu", clumptotal);

  /*
   * check that:
   *
   *   the overhead size (stuff up to CLUMP_OFFSET)
   * + clumptotal
   * + last clump marker (CHUNK_SIZE)
   * + any bytes we rounded off the end
   * = file size
   */
  if ((PMEM_CLUMP_OFFSET + clumptotal + (stbuf.st_size & (PMEM_CHUNK_SIZE - 1))
      + PMEM_CHUNK_SIZE) == (size_t) stbuf.st_size) {
    DEBUG("section sizes correctly add up to file size");
  } else {
    FATAL(
        "CLUMP_OFFSET %d + clumptotal %lu + rounded %d + "
        "PMEM_CHUNK_SIZE %lu = %lu, (not st_size %lu)",
        PMEM_CLUMP_OFFSET,
        clumptotal,
        (stbuf.st_size & (PMEM_CHUNK_SIZE - 1)),
        PMEM_CHUNK_SIZE,
        PMEM_CLUMP_OFFSET + clumptotal + (stbuf.st_size & (PMEM_CHUNK_SIZE - 1)) + PMEM_CHUNK_SIZE);
  }

  if (clp->size == 0)
    FATAL("no clumps found");

  while (clp->size) {
    size_t sz = clp->size & ~PMEM_STATE_MASK;
    int state = clp->size & PMEM_STATE_MASK;

    DEBUG("[%u]clump size %lu state %d", REL_PTR(clp, pmp), sz, state);
    if (sz > stats[PMEM_STATE_UNUSED].largest)
      stats[PMEM_STATE_UNUSED].largest = sz;
    if (stats[PMEM_STATE_UNUSED].smallest == 0
        || sz < stats[PMEM_STATE_UNUSED].smallest)
      stats[PMEM_STATE_UNUSED].smallest = sz;
    stats[PMEM_STATE_UNUSED].bytes += sz;
    stats[PMEM_STATE_UNUSED].count++;

    switch (state) {
      case PMEM_STATE_FREE:
        //DEBUG("clump state: free");
        break;

      case PMEM_STATE_RESERVED:
        //DEBUG("clump state: reserved");
        break;

      case PMEM_STATE_ACTIVE:
        //DEBUG("clump state: active");
        break;

      default:
        FATAL("unknown clump state: %d", state);
    }

    if (sz > stats[state].largest)
      stats[state].largest = sz;
    if (stats[state].smallest == 0 || sz < stats[state].smallest)
      stats[state].smallest = sz;
    stats[state].bytes += sz;
    stats[state].count++;

    clp = (struct clump *) ((uintptr_t) clp + sz);
    DEBUG("next clp 0x%lx, offset 0x%lx", clp, REL_PTR(clp, pmp));
  }

  if (clp == lastclp)
    DEBUG("all clump space accounted for");
  else
    FATAL("clump list stopped at %lx instead of %lx", clp, lastclp);

  if (munmap(pmp, stbuf.st_size) < 0)
    FATALSYS("munmap");

  // print the report
  printf("Summary of pmem pool:\n");
  printf("File size: %lu, %lu allocatable bytes in pool\n\n", stbuf.st_size,
         clumptotal);
  printf("     State      Bytes     Clumps    Largest   Smallest\n");
  for (i = 0; i < PMEM_STATE_UNUSED + 1; i++) {
    printf("%10s %10lu %10u %10lu %10lu\n", names[i], stats[i].bytes,
           stats[i].count, stats[i].largest, stats[i].smallest);
  }

}

// display pmem pool
void pmem_pool::pmemalloc_display() {
  struct clump* clp;
  size_t sz;
  size_t prev_sz;
  int state;
  clp = ABS_PTR((struct clump *) PMEM_CLUMP_OFFSET, pmp);

  fprintf(stdout, "--------------------------\n");
  while (1) {
    sz = clp->size & ~PMEM_STATE_MASK;
    prev_sz = clp->prevsize;
    state = clp->size & PMEM_STATE_MASK;
    fprintf(stdout, "%lu (%d)(%p)(%lu) -> ", sz, state, REL_PTR(clp, pmp),
            prev_sz);

    if (clp->size == 0)
      break;
    clp = (struct clump *) ((uintptr_t) clp + sz);
  }

  fprintf(stdout, "\n");
  fprintf(stdout, "--------------------------\n");
  fflush(stdout);
}

// pmemalloc_validate clump metadata
void pmem_pool::pmemalloc_validate(struct clump* clp) {
  size_t sz;
  struct clump* next;

  sz = clp->size & ~PMEM_STATE_MASK;
  if (sz == 0)
    return;

  next = (struct clump *) ((uintptr_t) clp + sz);

  if (sz != next->prevsize) {
    DEBUG("clp : %p clp->size : %lu lastfree : %p lastfree->prevsize : %lu",
        clp, sz, next, next->prevsize);
    pmemalloc_display();
    exit(EXIT_FAILURE);
  }
}

// check pmem pool health
void pmem_pool::pmemalloc_check() {
  struct clump *clp, *lastclp;
  clp = ABS_PTR((struct clump *) PMEM_CLUMP_OFFSET, pmp);

  lastclp = ABS_PTR(
      (struct clump *) (pool_size & ~(PMEM_CHUNK_SIZE - 1)) - PMEM_CHUNK_SIZE,
      pmp);

  if (clp->size == 0)
    FATAL("no clumps found");

  while (clp->size) {
    size_t sz = clp->size & ~PMEM_STATE_MASK;
    clp = (struct clump *) ((uintptr_t) clp + sz);
  }

  if (clp != lastclp) {
    pmemalloc_display();
    FATAL("clump list stopped at %lx instead of %lx", clp, lastclp);
  }
}

void debug(const char *file, int line, const char *func, const char *fmt, ...) {
  va_list ap;
  int save_errno;

  save_errno = errno;
  fprintf(stderr, "debug: %s:%d %s()", file, line, func);
  if (fmt) {
    fprintf(stderr, ": ");
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
  }
  fprintf(stderr, "\n");
  errno = save_errno;
}

void fatal(int err, const char *file, int line, const char *func,
           const char *fmt, ...) {
  va_list ap;

  fprintf(stderr, "ERROR: %s:%d %s()", file, line, func);
  if (fmt) {
    fprintf(stderr, ": ");
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
  }
  if (err)
    fprintf(stderr, ": %s", strerror(err));
  fprintf(stderr, "\n");
  exit(1);
}

}
