#pragma once

#include <string>
#include <getopt.h>
#include <vector>
#include <sys/time.h>

namespace storage {

// Logging
extern int level;

//#define NDG
#ifdef NDG
#define LOG_ERR(M, ...)
#define LOG_WARN(M, ...)
#define LOG_INFO(M, ...)
#else
#define clean_errno() (errno == 0 ? "" : strerror(errno))
#define LOG_ERR(M, ...)  if(level > 0) fprintf(stderr, "[EROR] [%s :: %s:%d] %s " M "\n", __FILE__, __func__, __LINE__, clean_errno(), ##__VA_ARGS__)
#define LOG_WARN(M, ...) if(level > 1) fprintf(stderr, "[WARN] [%s :: %s:%d] %s " M "\n",__FILE__, __func__,__LINE__, clean_errno(), ##__VA_ARGS__)
#define LOG_INFO(M, ...) if(level > 2) fprintf(stderr, "[INFO] [%s :: %s:%d] " M "\n", __FILE__, __func__, __LINE__, ##__VA_ARGS__)
#endif

enum engine_type {
  EE_INVALID,
  WAL,
  SP,
  LSM,
  OPT_WAL,
  OPT_SP,
  OPT_LSM
};

enum benchmark_type {
  BH_INVALID,
  YCSB,
  TPCC
};

class benchmark;

class config {
 public:
  std::string fs_path;
  benchmark** partitions;
  struct static_info* sp;

  int num_keys;
  int num_txns;

  bool single;
  int num_executors;

  bool read_only;

  double ycsb_per_writes;
  int ycsb_field_size;
  bool ycsb_update_one;
  int ycsb_tuples_per_txn;
  int ycsb_num_val_fields;
  double ycsb_skew;

  int tpcc_num_warehouses;
  bool tpcc_stock_level_only;

  int gc_interval;

  int merge_interval;
  double merge_ratio;

  bool verbose;
  bool recovery;
  bool storage_stats;

  int active_txn_threshold;
  int load_batch_size;

  engine_type etype;
  benchmark_type btype;
};

}
