// TESTER

#include <cassert>

#include "nstore.h"
#include "wal_coordinator.h"
#include "ycsb_benchmark.h"
#include "utils.h"

#include "libpm.h"

using namespace std;

// Global persistent memory structure
extern struct static_info *sp;

static void usage_exit(FILE *out) {
  fprintf(out, "Command line options : nstore <options> \n"
          "   -x --num-txns        :  Number of transactions to execute \n"
          "   -k --num-keys        :  Number of keys \n"
          "   -p --num-parts       :  Number of partitions \n"
          "   -w --per-writes      :  Percent of writes \n"
          "   -f --fs-path         :  Path for FS \n"
          "   -g --gc-interval     :  Group commit interval \n"
          "   -l --log-only        :  WAL only \n"
          "   -s --sp-only         :  SP only \n"
          "   -m --lsm-only        :  LSM only \n"
          "   -h --help            :  Print help message \n");
  exit(-1);
}

static struct option opts[] = {
    { "fs-path", optional_argument, NULL, 'f' },
    { "num-txns", optional_argument, NULL, 'x' },
    { "num-keys", optional_argument, NULL, 'k' },
    { "num-parts", optional_argument, NULL, 'p' },
    { "per-writes", optional_argument, NULL, 'w' },
    { "gc-interval", optional_argument, NULL, 'g' },
    { "log-only", no_argument, NULL, 'l' },
    { "sp-only", no_argument, NULL, 's' },
    { "lsm-only", no_argument, NULL, 'm' },
    { "verbose", no_argument, NULL, 'v' },
    { "skew", optional_argument, NULL, 'q' },
    { "help", no_argument, NULL, 'h' },
    { NULL, 0, NULL, 0 }
};

static void parse_arguments(int argc, char* argv[], config& state) {

  // Default Values
  state.fs_path = std::string("./");

  state.num_keys = 20;
  state.num_txns = 10;
  state.num_parts = 1;

  state.sz_value = 4;
  state.verbose = false;

  state.sz_tuple = 4 + 4 + state.sz_value + 10;

  state.gc_interval = 5;
  state.lsm_size = 1000;
  state.per_writes = 0.2;

  state.sp_only = false;
  state.log_only = false;
  state.lsm_only = false;

  state.skew = 0.1;

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "f:x:k:p:w:g:q:vlsmh", opts, &idx);

    if (c == -1)
      break;

    switch (c) {
      case 'f':
        state.fs_path = std::string(optarg);
        cout << "fs_path: " << state.fs_path << endl;
        break;
      case 'x':
        state.num_txns = atoi(optarg);
        cout << "num_txns: " << state.num_txns << endl;
        break;
      case 'k':
        state.num_keys = atoi(optarg);
        cout << "num_keys: " << state.num_keys << endl;
        break;
      case 'p':
        state.num_parts = atoi(optarg);
        cout << "num_parts: " << state.num_parts << endl;
        break;
      case 'v':
        state.verbose = true;
        break;
      case 'w':
        state.per_writes = atof(optarg);
        cout << "per_writes: " << state.per_writes << endl;
        break;
      case 'g':
        state.gc_interval = atoi(optarg);
        cout << "gc_interval: " << state.gc_interval << endl;
        break;
      case 'l':
        state.log_only = true;
        cout << "log_only: " << state.log_only << endl;
        break;
      case 's':
        state.sp_only = true;
        cout << "sp_only: " << state.sp_only << endl;
        break;
      case 'm':
        state.lsm_only = true;
        cout << "lsm_only: " << state.lsm_only << endl;
        break;
      case 'q':
        state.skew = atof(optarg);
        cout << "skew: " << state.skew << endl;
        break;
      case 'h':
        usage_exit(stderr);
        break;
      default:
        fprintf(stderr, "\nUnknown option: -%c-\n", c);
        usage_exit(stderr);
    }
  }

  assert(state.per_writes >= 0 && state.per_writes <= 1);
}


int main(int argc, char **argv) {
  const char* path = "./testfile";

  long pmp_size = 1024 * 1024 * 1024;
  if ((pmp = pmemalloc_init(path, pmp_size)) == NULL)
    cout << "pmemalloc_init on :" << path << endl;

  sp = (struct static_info *) pmemalloc_static_area();

  // Start
  config state;
  parse_arguments(argc, argv, state);
  state.sp = sp;

  // Generate Zipf dist
  long range_size = state.num_keys / state.num_parts;
  long range_txns = state.num_txns / state.num_parts;
  simple_skew(state.zipf_dist, range_size, range_txns);
  uniform(state.uniform_dist, range_txns);

  if (state.sp_only == false && state.lsm_only == false) {
    cout << "WAL :: " << endl;

    ycsb_benchmark ycsb(state);
    wal_coordinator wal(state);

    wal.runner(ycsb.get_dataset());
    wal.runner(ycsb.get_workload());
  }

  /*
   if (state.log_only == false && state.lsm_only == false) {
   cout << "SP  :: ";
   sp_engine sp(state);
   sp.test();
   }

   if (state.log_only == false && state.sp_only == false) {
   cout << "LSM :: ";
   lsm_engine lsm(state);
   lsm.test();
   }
   */

  return 0;
}
