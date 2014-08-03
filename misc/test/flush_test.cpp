#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <errno.h>
#include <sys/time.h>

#include <iostream>
#include <chrono>
#include <random>
#include <string>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <algorithm>
#include <iomanip>

using namespace std;

#define VALUE_SIZE 4096
#define ITR        1024*32

std::string get_random_string(size_t length) {
  auto randchar = []() -> char
  {
    const char charset[] =
    "0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz";
    const size_t max_index = (sizeof(charset) - 1);
    return charset[ rand() % max_index ];
  };
  std::string str(length, 0);
  std::generate_n(str.begin(), length, randchar);
  return str;
}

#define ALIGN 64  /* assumes 64B cache line size */
#define DEBUG

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
  //pmem_drain_pm_stores();
}

int main(int argc, char *argv[]) {
  int fd, offset;
  char *data;
  struct stat sbuf;
  FILE* fp;
  timeval t1, t2, diff;
  timeval total;
  std::string buf;
  double duration;

  std::string name = "/mnt/pmfs/n-store/log";

  if ((fp = fopen(name.c_str(), "w")) == NULL) {
    perror("open");
    exit(EXIT_FAILURE);
  }

  fd = fileno(fp);

  // fsync
  total = (struct timeval ) { 0 };

  for (int itr = 0; itr < ITR; itr++) {
    buf = get_random_string(VALUE_SIZE);

    gettimeofday(&t1, 0);

    fwrite(buf.c_str(), 1, buf.size(), fp);
    fsync(fd);

    gettimeofday(&t2, 0);
    timersub(&t2, &t1, &diff);
    timeradd(&diff, &total, &total);
  }

  duration = (total.tv_sec) * 1000.0;      // sec to ms
  duration += (total.tv_usec) / 1000.0;      // us to ms

  cout << std::fixed << std::setprecision(2);
  cout << "FSYNC :: Duration(s) : " << (duration / 1000.0) << endl;

  // clflush

  total = (struct timeval ) { 0 };

  for (int itr = 0; itr < ITR; itr++) {
    buf = get_random_string(VALUE_SIZE);

    gettimeofday(&t1, 0);

    pmem_persist((void*) buf.c_str(), buf.size(), 0);

    gettimeofday(&t2, 0);
    timersub(&t2, &t1, &diff);
    timeradd(&diff, &total, &total);
  }

  duration = (total.tv_sec) * 1000.0;      // sec to ms
  duration += (total.tv_usec) / 1000.0;      // us to ms

  cout << std::fixed << std::setprecision(2);
  cout << "CLFLUSH :: Duration(s) : " << (duration / 1000.0) << endl;

}
