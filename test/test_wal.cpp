// TESTER

#include <cstdio>
#include <cassert>
#include <unistd.h>

#include "nstore.h"
#include "wal_engine.h"
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
  state.sz_value = 4;
  state.per_writes = 0.2;
  state.skew = 0.1;
  state.sp = sp;

  ycsb_benchmark ycsb(state);
  wal_engine wal(state);
  wal.generator(ycsb.get_dataset(), false);
  wal.generator(ycsb.get_workload(), true);

  int ret = std::remove(path);
  assert(ret == 0);

  return 0;
}
