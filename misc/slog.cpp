#include <cstdio>
#include <errno.h>
#include <cstdarg>
#include <string.h>

#include <vector>
#include <thread>

using namespace std;

int level = 2; // verbosity level

#ifdef NDEBUG
#define LOG_ERR(M, ...)
#define LOG_WARN(M, ...)
#define LOG_INFO(M, ...)
#else
#define clean_errno() (errno == 0 ? "" : strerror(errno))
#define LOG_ERR(M, ...)  if(level > 0) fprintf(stderr, "[EROR] [%s :: %s:%d] %s " M "\n", __FILE__, __func__, __LINE__, clean_errno(), ##__VA_ARGS__)
#define LOG_WARN(M, ...) if(level > 1) fprintf(stderr, "[WARN] [%s :: %s:%d] %s " M "\n",__FILE__, __func__,__LINE__, clean_errno(), ##__VA_ARGS__)
#define LOG_INFO(M, ...) if(level > 2) fprintf(stderr, "[INFO] [%s :: %s:%d] " M "\n", __FILE__, __func__, __LINE__, ##__VA_ARGS__)
#endif

void work() {
  int cnt = 16 * 1024;

  for (int i = 0; i < cnt; i++) {

    void* pmp = (void*) 0x1000034;
    size_t sz = 123;
    int state = 50;

    LOG_ERR("pmp: %p clump size %lu state %d", pmp, sz, state);
    LOG_WARN("warn: %lu", sz);
  }

}

int main() {
  std::vector<std::thread> executors;
  int num_thds = 4;

  for (int i = 0; i < num_thds; i++)
    executors.push_back(std::thread(work));

  for (int i = 0; i < num_thds; i++)
    executors[i].join();

}

