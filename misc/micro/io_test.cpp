#include <iostream>
#include <fcntl.h>
#include <string>
#include <cstdio>
#include <cstdlib>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstring>
#include <cassert>
#include <getopt.h>

using namespace std;

#define roundup2(x, y)  (((x)+((y)-1))&(~((y)-1))) /* if y is powers of two */

class timer {
 public:

  timer() {
    total = timeval();
  }

  double duration() {
    double duration;

    duration = (total.tv_sec) * 1000.0;      // sec to ms
    duration += (total.tv_usec) / 1000.0;      // us to ms

    return duration;
  }

  void start() {
    gettimeofday(&t1, NULL);
  }

  void end() {
    gettimeofday(&t2, NULL);
    timersub(&t2, &t1, &diff);
    timeradd(&diff, &total, &total);
  }

  void reset() {
    total = timeval();
  }

  timeval t1, t2, diff;
  timeval total;
};

#define ALIGN 64

static inline void pmem_flush_cache(void *addr, size_t len,
                                    __attribute__((unused)) int flags) {
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

#define MAX_BUF_SIZE    4096

static void usage_exit(FILE *out) {
  fprintf(
      out,
      "Command line options : nstore <options> \n"
      "   -h --help              :  Print help message \n"
      "   -r --random-mode       :  Random accesses \n"
      "   -c --chunk-size        :  Chunk size \n"
      "   -f --fs-type           :  FS type (0 : NVM, 1: PMFS, 2: EXT4, 3:TMPFS \n");
  exit(EXIT_FAILURE);
}

static struct option opts[] = { { "random-mode", no_argument, NULL, 'r' }, {
    "fs-type", optional_argument, NULL, 'f' }, { "chunk-size",
optional_argument, NULL, 'c' }, { "help", no_argument, NULL, 'h' }, { NULL, 0,
NULL, 0 } };

class config {
 public:

  bool random_mode;
  bool sync_mode;
  bool nvm_mode;
  int fs_type;
  int chunk_size;

  std::string fs_path;
};

static void parse_arguments(int argc, char* argv[], config& state) {

  // Default Values
  state.fs_type = 1;
  state.fs_path = "/mnt/pmfs/n-store/";

  state.random_mode = false;
  state.sync_mode = false;
  state.nvm_mode = false;
  state.chunk_size = 64;

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "f:c:hr", opts, &idx);

    if (c == -1)
      break;

    switch (c) {
      case 'f':
        state.fs_type = atoi(optarg);
        break;

      case 'r':
        state.random_mode = true;
        cout << "random_mode " << endl;
        break;

      case 's':
        state.sync_mode = true;
        cout << "sync_mode " << endl;
        break;

      case 'c':
        state.chunk_size = atoi(optarg);
        cout << "chunk_size : " << state.chunk_size << endl;
        break;

      case 'h':
        usage_exit(stderr);
        break;
      default:
        fprintf(stderr, "\nUnknown option: -%c-\n", c);
        usage_exit(stderr);
    }
  }

  switch (state.fs_type) {
    case 0:
      state.nvm_mode = true;
      cout << "nvm_mode " << endl;
      break;

    case 1:
      state.fs_path = "/mnt/pmfs/n-store/";
      cout << "fs_path : " << state.fs_path << endl;
      break;

    case 2:
      state.fs_path = "./";
      cout << "fs_path : " << state.fs_path << endl;
      break;

    case 3:
      state.fs_path = "/tmp/tmpdir/";
      cout << "fs_path : " << state.fs_path << endl;
      break;

    default:
      fprintf(stderr, "\nUnknown fs_type : -%d-\n", state.fs_type);
      usage_exit(stderr);
  }

}

int main(int argc, char **argv) {

  config state;
  parse_arguments(argc, argv, state);

  FILE* fp = NULL;
  int fd = -1;
  size_t chunk_size, file_size, offset;
  unsigned int i, j, itr_cnt;
  timer tm;

  file_size = 2UL * 1024 * 1024 * 1024;  // 2 GB

  bool random_mode = state.random_mode;
  bool sync_mode = state.sync_mode;
  bool nvm_mode = state.nvm_mode;
  std::string fs_prefix = state.fs_path;
  chunk_size = state.chunk_size;
  std::string path = state.fs_path;
  double iops;

  itr_cnt = 128 * 128 * 4;

  size_t min_chunk_size = 8;
  size_t max_chunk_size = 32 * 1024;

  char buf[max_chunk_size + 1];
  for (j = 0; j < max_chunk_size; j++)
    buf[j] = ('a' + rand() % 5);

  offset = 0;

  for (int random = 0; random <= 1; random++) {
    random_mode = (bool) random;
    printf("RANDOM : %d \n", random);

    for (chunk_size = min_chunk_size; chunk_size <= max_chunk_size;
        chunk_size *= 4) {
      printf("CHUNK SIZE : %lu \n", chunk_size);

      // NVM MODE
      char* nvm_buf = new char[file_size + chunk_size + 1];

      // WRITE
      for (j = 0; j < itr_cnt; j++) {

        if (random_mode) {
          offset = rand() % file_size;
        }

        tm.start();

        memcpy((void*) (nvm_buf + offset), buf, chunk_size);
        pmem_persist(nvm_buf + offset, chunk_size, 0);

        tm.end();
      }

      iops = (itr_cnt * 1000) / tm.duration();
      printf("IOPS : %lf \n", iops);
      printf("BW   : %lf \n", iops * chunk_size);

      delete nvm_buf;

      // FS MODE
      path = fs_prefix + "io_file";
      int ret;

      fp = fopen(path.c_str(), "w+");
      fd = fileno(fp);
      assert(fp != NULL);
      posix_fallocate(fd, 0, file_size);

      // WRITE
      for (j = 0; j < itr_cnt; j++) {

        if (random_mode) {
          offset = rand() % file_size;
          offset = roundup2(offset, MAX_BUF_SIZE);

          tm.start();
          ret = fseek(fp, offset, SEEK_SET);
          tm.end();

          if (ret != 0) {
            perror("fseek");
            exit(EXIT_FAILURE);
          }
        }

        tm.start();
        ret = write(fd, buf, chunk_size);
        tm.end();

        if (ret <= 0) {
          perror("write");
          exit(EXIT_FAILURE);
        }

        tm.start();
        ret = fsync(fd);
        tm.end();

        if (ret != 0) {
          perror("fsync");
          exit(EXIT_FAILURE);
        }
      }

      iops = (itr_cnt * 1000) / tm.duration();
      printf("IOPS : %lf \n", iops);
      printf("BW   : %lf \n", iops * chunk_size);
      fclose(fp);

    }
  }

  return 0;
}
