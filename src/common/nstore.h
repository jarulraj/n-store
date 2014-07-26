#ifndef NSTORE_H_
#define NSTORE_H_

#include <string>
#include <getopt.h>
#include <vector>
#include <sys/time.h>

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
    { "log-enable", no_argument, NULL, 'a' },
    { "sp-enable", no_argument, NULL, 's' },
    { "lsm-enable", no_argument, NULL, 'm' },
    { "opt-wal-enable", no_argument, NULL, 'w' },
    { "opt-sp-enable", no_argument, NULL, 'c' },
    { "opt-lsm-enable", no_argument, NULL, 'l' },
    { "fs-path", optional_argument, NULL, 'f' },
    { "num-txns", optional_argument, NULL, 'x' },
    { "num-keys", optional_argument, NULL, 'k' },
    { "num-executors", optional_argument, NULL, 'e' },
    { "ycsb_per_writes", optional_argument, NULL, 'w' },
    { "ycsb_skew", optional_argument, NULL, 'q' },
    { "gc-interval", optional_argument, NULL, 'g' },
    { "verbose", no_argument, NULL, 'v' },
    { "help", no_argument, NULL, 'h' },
    { "ycsb-update-one", no_argument, NULL, 'u' },
    { NULL, 0, NULL, 0 }
};

enum engine_type {
  EE_INVALID,
  WAL,
  SP,
  LSM,
  OPT_WAL,
  OPT_SP,
  OPT_LSM
};

class database;

class config {
 public:
  std::string fs_path;

  struct static_info* sp;
  database* db;

  int num_keys;
  int num_txns;
  int num_executors;

  bool read_only;

  double ycsb_per_writes;
  int ycsb_field_size;
  bool ycsb_update_one;
  int ycsb_tuples_per_txn;
  int ycsb_num_val_fields;
  double ycsb_skew;

  int gc_interval;

  int merge_interval;
  double merge_ratio;

  bool verbose;
  engine_type etype;
};

#endif /* NSTORE_H_ */
