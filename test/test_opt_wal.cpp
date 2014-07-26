// TESTER

#include <cstdio>
#include <cassert>
#include <unistd.h>

#include "nstore.h"
#include "opt_wal_engine.h"
#include "ycsb_benchmark.h"
#include "utils.h"
#include "libpm.h"

using namespace std;

extern struct static_info* sp;
int level = 0;

int main(int argc, char **argv) {
  const char* path = "./zfile";

  // cleanup
  unlink(path);

  long pmp_size = 1024 * 1024 * 1024;
  if ((pmp = pmemalloc_init(path, pmp_size)) == NULL)
    cout << "pmemalloc_init on :" << path << endl;

  sp = (struct static_info *) pmemalloc_static_area();

  // Start
  config state;
  state.fs_path = std::string("./");
  state.num_keys = 20;
  state.num_txns = 10;
  state.num_executors = 1;
  state.ycsb_field_size = 4;
  state.ycsb_per_writes = 0.2;
  state.ycsb_skew = 0.1;
  state.ycsb_tuples_per_txn = 2;
  state.sp = sp;

  ycsb_benchmark ycsb(state);
  /*
  opt_wal_engine* opt_wal ;

  opt_wal = new opt_wal_engine(state);
  ycsb.load(opt_wal);
  delete opt_wal;
  */

  /*
  opt_wal = new opt_wal_engine(state);
  ycsb.execute(opt_wal);
  delete opt_wal;
  */

  int ret = std::remove(path);
  assert(ret == 0);

  return 0;
}
