#ifndef NSTORE_H_
#define NSTORE_H_

#include <string>
#include <getopt.h>
#include <vector>

#include "database.h"

using namespace std;

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
  bool log_only;
  bool sp_only;
  bool lsm_only;
};

#endif /* NSTORE_H_ */
