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


class config {

 public:
  std::string fs_path;

  struct static_info* sp;
  database* db;

  int num_keys;
  int num_txns;
  int num_executors;

  int sz_value;
  int sz_tuple;

  double per_writes;

  int gc_interval;
  int lsm_size;

  double skew;

  bool verbose;
  bool log_enable;
  bool aries_enable;
  bool sp_enable;
  bool lsm_enable;
};

#endif /* NSTORE_H_ */
