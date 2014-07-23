#ifndef NSTORE_H_
#define NSTORE_H_

#include <string>
#include <getopt.h>
#include <vector>
#include <sys/time.h>

#include "database.h"

using namespace std;

// Logging
extern int level;

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

static struct option opts[] = {
    { "fs-path", optional_argument, NULL, 'f' },
    { "num-txns", optional_argument, NULL, 'x' },
    { "num-keys", optional_argument, NULL, 'k' },
    { "num-executors", optional_argument, NULL, 'e' },
    { "per-writes", optional_argument, NULL, 'w' },
    { "gc-interval", optional_argument, NULL, 'g' },
    { "log-enable", no_argument, NULL, 'a' },
    { "opt-wal-enable", no_argument, NULL, 'w' },
    { "sp-enable", no_argument, NULL, 's' },
    { "opt-sp-enable", no_argument, NULL, 'c' },
    { "lsm-enable", no_argument, NULL, 'm' },
    { "opt-lsm-enable", no_argument, NULL, 'l' },
    { "verbose", no_argument, NULL, 'v' },
    { "skew", optional_argument, NULL, 'q' },
    { "help", no_argument, NULL, 'h' },
    { NULL, 0, NULL, 0 }
};

class config {

 public:
  std::string fs_path;

  struct static_info* sp;
  database* db;

  int num_keys;
  int num_txns;
  int num_executors;

  int sz_value;

  double per_writes;

  int gc_interval;

  int merge_interval;
  double merge_ratio;

  double skew;

  bool verbose;
  bool opt_wal_enable;
  bool wal_enable;
  bool sp_enable;
  bool opt_sp_enable;
  bool lsm_enable;
  bool opt_lsm_enable;

};

#endif /* NSTORE_H_ */
