// TESTER

#include <cassert>

#include "nstore.h"
#include "wal_engine.h"
#include "aries_engine.h"
#include "sp_engine.h"

#include "ycsb_benchmark.h"
#include "utils.h"

#include "libpm.h"

using namespace std;

extern struct static_info *sp;  // global persistent memory structure
int level = 2;  // verbosity level

static void usage_exit(FILE *out) {
  fprintf(out, "Command line options : nstore <options> \n"
          "   -x --num-txns        :  Number of transactions \n"
          "   -k --num-keys        :  Number of keys \n"
          "   -e --num-executors   :  Number of executors \n"
          "   -w --per-writes      :  Percent of writes \n"
          "   -f --fs-path         :  Path for FS \n"
          "   -g --gc-interval     :  Group commit interval \n"
          "   -l --log-enable      :  WAL enable \n"
          "   -s --sp-enable       :  SP enable \n"
          "   -m --lsm-enable      :  LSM enable \n"
          "   -q --skew            :  Skew \n"
          "   -h --help            :  Print help message \n");
  exit(-1);
}

static struct option opts[] = { { "fs-path", optional_argument, NULL, 'f' }, {
    "num-txns", optional_argument, NULL, 'x' }, { "num-keys", optional_argument,
NULL, 'k' }, { "num-executors", optional_argument, NULL, 'e' }, { "per-writes",
optional_argument, NULL, 'w' }, { "gc-interval",
optional_argument, NULL, 'g' }, { "log-enable", no_argument, NULL, 'l' }, {
    "sp-enable", no_argument, NULL, 's' }, { "aries-enable", no_argument, NULL,
    'a' }, { "lsm-enable", no_argument, NULL, 'm' }, { "verbose", no_argument,
NULL, 'v' }, { "skew", optional_argument, NULL, 'q' }, { "help",
no_argument, NULL, 'h' }, { NULL, 0, NULL, 0 } };

static void parse_arguments(int argc, char* argv[], config& state) {

  // Default Values
  state.fs_path = std::string("./");

  state.num_keys = 10;
  state.num_txns = 10;
  state.num_executors = 1;

  state.sz_value = 1024;
  state.verbose = false;

  state.gc_interval = 5;
  state.per_writes = 0.1;

  state.lsm_size = 1000;

  state.sp_enable = false;
  state.aries_enable = false;
  state.log_enable = false;
  state.lsm_enable = false;

  state.skew = 1;

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "f:x:k:e:w:g:q:vlsmah", opts, &idx);

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
      case 'e':
        state.num_executors = atoi(optarg);
        cout << "num_executors: " << state.num_executors << endl;
        break;
      case 'v':
        state.verbose = true;
        level = 3;
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
        state.log_enable = true;
        cout << "log_enable: " << state.log_enable << endl;
        break;
      case 's':
        state.sp_enable = true;
        cout << "sp_enable: " << state.sp_enable << endl;
        break;
      case 'm':
        state.lsm_enable = true;
        cout << "lsm_enable: " << state.lsm_enable << endl;
        break;
      case 'a':
        state.aries_enable = true;
        cout << "aries_enable: " << state.aries_enable << endl;
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
  const char* path = "/mnt/pmfs/n-store/zfile";
  //const char* path = "./zfile";

  size_t pmp_size = 4UL * 1024 * 1024 * 1024;
  if ((pmp = pmemalloc_init(path, pmp_size)) == NULL)
    cout << "pmemalloc_init on :" << path << endl;

  sp = (struct static_info *) pmemalloc_static_area();

  // Start
  config state;
  parse_arguments(argc, argv, state);
  state.sp = sp;

  if (state.log_enable == true) {
    LOG_INFO("WAL");

    bool generate_dataset = !sp->init;
    ycsb_benchmark ycsb(state);
    wal_engine wal(state);

    if (generate_dataset)
      wal.generator(ycsb.get_dataset(), false);
    wal.generator(ycsb.get_workload(), true);
  }

  if (state.aries_enable == true) {
    LOG_INFO("ARIES");

    bool generate_dataset = !sp->init;
    ycsb_benchmark ycsb(state);
    aries_engine aries(state);

    aries.generator(ycsb.get_dataset(), false);
    aries.generator(ycsb.get_workload(), true);
  }

  if (state.sp_enable == true) {
    LOG_INFO("SP");

    bool generate_dataset = !sp->init;
    ycsb_benchmark ycsb(state);
    sp_engine sp(state);

    if (generate_dataset)
      sp.generator(ycsb.get_dataset(), false);
    sp.generator(ycsb.get_workload(), true);
  }

  return 0;
}
